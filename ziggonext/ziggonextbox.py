"""ZiggoNextBox"""
import paho.mqtt.client as mqtt
import urllib.parse
from paho.mqtt.client import Client
import json
import requests
from logging import Logger
import random
import time
import sys, traceback
from .models import ZiggoNextSession, ZiggoNextBoxPlayingInfo, ZiggoChannel
from .const import (
    BOX_PLAY_STATE_BUFFER,
    BOX_PLAY_STATE_CHANNEL,
    BOX_PLAY_STATE_DVR,
    BOX_PLAY_STATE_REPLAY,
    BOX_PLAY_STATE_APP,
    BOX_PLAY_STATE_VOD,
    
    ONLINE_RUNNING,
    ONLINE_STANDBY,
    UNKNOWN,
    MEDIA_KEY_PLAY_PAUSE,
    MEDIA_KEY_CHANNEL_DOWN,
    MEDIA_KEY_CHANNEL_UP,
    MEDIA_KEY_POWER,
    COUNTRY_URLS_HTTP,
    COUNTRY_URLS_MQTT
)
DEFAULT_PORT = 443

def _makeId(stringLength=10):
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(random.choice(letters) for i in range(stringLength))

class ZiggoNextBox:
    
    box_id: str
    name: str
    state: str = UNKNOWN
    info: ZiggoNextBoxPlayingInfo
    available: bool = False
    channels: ZiggoChannel = {}

    def __init__(self, box_id:str, name:str, householdId:str, token:str, country_code:str, logger:Logger, mqttClient:Client, client_id:str):
        self.box_id = box_id
        self.name = name
        self._householdId = householdId
        self._token = token
        self.info = ZiggoNextBoxPlayingInfo()
        self.logger = logger
        self._mqttClientConnected = False
        self._createUrls(country_code)
        self.mqttClientId = client_id
        self.mqttClient = mqttClient
        self._change_callback = None
        
    def _createUrls(self, country_code: str):
        baseUrl = COUNTRY_URLS_HTTP[country_code]
        self._api_url_listing_format =  baseUrl + "/listings/{id}"
        self._api_url_mediagroup_format =  baseUrl + "/mediagroups/{id}"
        self._mqtt_broker = COUNTRY_URLS_MQTT[country_code]
    
    def register(self):
        self._do_subscribe("#")
        self._do_subscribe(self._householdId)
        self._do_subscribe(self._householdId + "/+/status")
        self._do_subscribe(self._householdId + "/+/localRecordings")
        self._do_subscribe(self._householdId + "/+/localRecordings/capacity")
        payload = {
                "source": self.mqttClientId,
                "state": "ONLINE_RUNNING",
                "deviceType": "HGO",
            }
        register_topic = self._householdId + "/" + self.mqttClientId + "/status"
        self.mqttClient.publish(register_topic, json.dumps(payload))
    
    def set_callback(self, callback):
        self._change_callback = callback

    def _do_subscribe(self, topic):
        """Subscribes to mqtt topic"""
        self.mqttClient.subscribe(topic)
        self.logger.debug("subscribed to topic: {topic}".format(topic=topic))
    
    def _update_settopbox_state(self, payload):
        """Registers a new settop box"""
        deviceId = payload["source"]
        if deviceId != self.box_id:
            return
        state = payload["state"]
        
        if self.state == UNKNOWN:
            self._request_settop_box_state() 
            self._do_subscribe(self._householdId + "/" + self.mqttClientId)
            baseTopic = self._householdId + "/" + self.box_id
            self._do_subscribe(baseTopic)
            self._do_subscribe(baseTopic + "/status")
        if state == ONLINE_STANDBY :
            self.info = ZiggoNextBoxPlayingInfo()
        else:
            self._request_settop_box_state()
        self.state = state
        if self._change_callback:
            self._change_callback()
               
    def _request_settop_box_state(self):
        """Sends mqtt message to receive state from settop box"""
        self.logger.debug("Request box state for box " + self.name)
        topic = self._householdId + "/" + self.box_id
        payload = {
            "id": _makeId(8),
            "type": "CPE.getUiStatus",
            "source": self.mqttClientId,
        }
        self.mqttClient.publish(topic, json.dumps(payload))
    
    def update_settop_box(self, payload):
        """Updates settopbox state"""
        deviceId = payload["source"]
        if deviceId != self.box_id:
            return
        self.logger.debug(f"Updating box {self.box_id} with payload:")
        self.logger.debug(payload)
        statusPayload = payload["status"]
        if not "uiStatus" in statusPayload:
            self.logger.debug("Unexpected statusPayload: ")
            self.logger.debug(statusPayload)
            return
        uiStatus = statusPayload["uiStatus"]
        if uiStatus == "mainUI":
            playerState = statusPayload["playerState"]
            if not "sourceType" in playerState:
                return
            sourceType = playerState["sourceType"]
            stateSource = playerState["source"]
            speed = playerState["speed"]
            if self.info is None:
                self.info = ZiggoNextBoxPlayingInfo()
            if sourceType == BOX_PLAY_STATE_REPLAY:
                self.info.setSourceType(BOX_PLAY_STATE_REPLAY)
                eventId = stateSource["eventId"]
                listing = self._get_listing(eventId)
                channel_id = self._get_listing_channel_id(listing)
                channel = self.channels[channel_id]
                self.info.setChannel(channel_id)
                self.info.setChannelTitle(channel.title)
                self.info.setTitle(
                    "ReplayTV: " + self._get_listing_title(listing)
                )
                self.info.setImage(self._get_listing_image(listing))
                self.info.setPaused(speed == 0)
            elif sourceType == BOX_PLAY_STATE_DVR:
                self.info.setSourceType(BOX_PLAY_STATE_DVR)
                recordingId = stateSource["recordingId"]
                listing = self._get_listing(recordingId)
                self.logger.info(self.channels)
                channel_id = self._get_listing_channel_id(listing)
                self.logger.info(self.channels)
                channel = self.channels[channel_id]
                self.info.setChannel(channel_id)
                self.info.setChannelTitle(channel.title)
                self.info.setTitle("Recording: " + self._get_listing_title(listing))
                self.info.setImage(
                    self._get_listing_image(listing)
                )
                self.info.setPaused(speed == 0)
            elif sourceType == BOX_PLAY_STATE_BUFFER:
                self.info.setSourceType(BOX_PLAY_STATE_BUFFER)
                channelId = stateSource["channelId"]
                channel = self.channels[channelId]
                eventId = stateSource["eventId"]
                listing = self._get_listing(eventId)
                self.info.setChannel(channelId)
                self.info.setChannelTitle(channel.title)
                self.info.setTitle(
                    "Delayed: " + self._get_listing_title(listing)
                )
                self.info.setImage(channel.streamImage)
                self.info.setPaused(speed == 0)
            elif playerState["sourceType"] == BOX_PLAY_STATE_CHANNEL:
                self.info.setSourceType(BOX_PLAY_STATE_CHANNEL)
                channelId = stateSource["channelId"]
                eventId = stateSource["eventId"]
                channel = self.channels[channelId]
                listing = self._get_listing(eventId)
                self.info.setChannel(channelId)
                self.info.setChannelTitle(channel.title)
                self.info.setTitle(self._get_listing_title(listing))
                self.info.setImage(channel.streamImage)
                self.info.setPaused(False)
            elif playerState["sourceType"] == BOX_PLAY_STATE_VOD:
                self.info.setSourceType(BOX_PLAY_STATE_VOD)
                title_id = stateSource["titleId"]
                mediagroup_content = self._get_mediagroup(title_id)
                self.info.setChannel(None)
                self.info.setChannelTitle("VOD")
                self.info.setTitle(mediagroup_content["title"])
                self.info.setImage(self._get_mediagroup_image(mediagroup_content))
                self.info.setPaused(speed == 0)
            else:
                self.info.setSourceType(BOX_PLAY_STATE_CHANNEL)
                eventId = stateSource["eventId"]
                self.info.setChannel(None)
                self.info.setTitle("Playing something...")
                self.info.setImage(None)
                self.info.setPaused(speed == 0)
        elif uiStatus == "apps":
            appsState = statusPayload["appsState"]
            logoPath = appsState["logoPath"]
            if not logoPath.startswith("http:"):
                logoPath = "https:" + logoPath
            self.info.setSourceType(BOX_PLAY_STATE_APP)
            self.info.setChannel(None)
            self.info.setChannelTitle(appsState["appName"])
            self.info.setTitle(appsState["appName"])
            self.info.setImage(logoPath)
            self.info.setPaused(False)
    
        if self._change_callback:
            self._change_callback()
    
    def _get_listing_title(self, listing_content):
        """Get listing title."""
        if listing_content is None:
            return ""
        return listing_content["program"]["title"]

    
    def _get_listing_image(self, listing_content):
        """Get listing image."""
        return listing_content["program"]["images"][0]["url"]

    def _get_listing_channel_id(self, listing_content):
        """Get listing channelId."""
        return listing_content["stationId"].replace("lgi-nl-prod-master:","").replace("lgi-be-prod-master:","")
    
    def _get_listing(self, listing_id):
        response = requests.get(self._api_url_listing_format.format(id=listing_id))
        if response.status_code == 200:
            return response.json()
        return None

    def _get_mediagroup(self, title_id):
        response = requests.get(self._api_url_mediagroup_format.format(id=title_id))
        if response.status_code == 200:
            return response.json()
        return None
    
    def _get_mediagroup_image(self, mediagroup_content):
        return mediagroup_content["images"][0]["url"]
    
    def send_key_to_box(self,key: str):
        """Sends emulated (remote) key press to settopbox"""
        payload = (
            '{"type":"CPE.KeyEvent","status":{"w3cKey":"'
            + key
            + '","eventType":"keyDownUp"}}'
        )
        self.mqttClient.publish(self._householdId+ "/" + self.box_id, payload)
        self._request_settop_box_state()
    
    def set_channel(self, serviceId):
        payload = (
            '{"id":"'
            + _makeId(8)
            + '","type":"CPE.pushToTV","source":{"clientId":"'
            + self.mqttClientId
            + '","friendlyDeviceName":"Home Assistant"},"status":{"sourceType":"linear","source":{"channelId":"'
            + serviceId
            + '"},"relativePosition":0,"speed":1}}'
        )

        self.mqttClient.publish(self._householdId + "/" + self.box_id, payload)
        self._request_settop_box_state()

    def play_recording(self, recordingId):
        payload = (
            '{"id":"'
            + _makeId(8)
            + '","type":"CPE.pushToTV","source":{"clientId":"'
            + self.mqttClientId
            + '","friendlyDeviceName":"Home Assistant"},"status":{"sourceType":"nDVR","source":{"recordingId":"'
            + recordingId
            + '"},"relativePosition":0}}'
        )

        self.mqttClient.publish(self._householdId + "/" + self.box_id, payload)
        self._request_settop_box_state()
    
    def turn_off(self):
        self.info = ZiggoNextBoxPlayingInfo()