#!/usr/bin/python
# Finds a doms radio/tv program given a prefix of its tvmeter metadata string
import sys, config, os, urllib, urllib2, re
from lxml import etree


PID_XPATH = "/types:result/types:resultList/types:objectFields/types:pid/text()"
START_XPATH = "/pb:programBroadcast/pb:timeStart/text()"
STOP_XPATH = "/pb:programBroadcast/pb:timeStop/text()"
VALID_XPATH = "/validation/@valid"
NAMESPACES = {
    "types" : "http://www.fedora.info/definitions/1/0/types/",
    "pb" : "http://doms.statsbiblioteket.dk/types/program_broadcast/0/1/#"
    }


if __name__ == "__main__":
    config.parseOpts(sys.argv[0:])

    with open("./doms_corrupt.csv", "r") as tvmeter_lines:
        for tvmeter_line in tvmeter_lines:
            if (tvmeter_line[0]) == '"':
                testString = tvmeter_line[1:51]
            else:
                testString = tvmeter_line[:50]
            query = urllib.urlencode({
                'query' : 'identifier~' + testString + '*',
                'pid' : 'true',
                'resultFormat' : 'xml'
            })
            url = "http://" + config.doms_server + ":" + str(
                config.doms_port) + "/fedora/objects?query=" + urllib.quote_plus("identifier~'" + testString + "*'") + "&pid=true&resultFormat=xml"
            request = urllib2.Request(url)
            if (config.debug):
                print url
                sys.stdout.flush()
            try:
                data = urllib2.urlopen(request)
                string = data.read()
                data.close()
                if (config.debug):
                    print string
                data_xml = etree.fromstring(string)
                pid_result = data_xml.xpath(PID_XPATH, namespaces=NAMESPACES)
                if len(pid_result) == 0:
                    print "No match for " + url
                else:
                    for pid in pid_result:
                        if (config.debug):
                            print pid
                        broadcast_url = "http://" + config.doms_server + ":" + str(
                            config.doms_port) + "/fedora/objects/" + urllib.quote(pid) + "/datastreams/PROGRAM_BROADCAST/content"
                        if (config.debug):
                            print broadcast_url
                        request = urllib2.Request(broadcast_url)
                        try:
                            data = urllib2.urlopen(request)
                            string = data.read()
                            data.close()
                            if (config.debug):
                                print string
                            broadcastdata_xml = etree.fromstring(string)
                            start_result = broadcastdata_xml.xpath(START_XPATH, namespaces=NAMESPACES)
                            stop_result = broadcastdata_xml.xpath(STOP_XPATH, namespaces=NAMESPACES)
                            print pid + " " + start_result[0] + " " + stop_result[0] + " " + testString
                            sys.stdout.flush()
                        except:
                            validation_url = "http://" + config.doms_server + ":" + str(
                                                    config.doms_port) + "/fedora/objects/" + urllib.quote(pid) + "/validate"
                            request = urllib2.Request(validation_url)
                            try:
                                data = urllib2.urlopen(request)
                                string = data.read()
                                data.close()
                                if (config.debug):
                                    print string
                                validation_xml = etree.fromstring(string)
                                validation_result = validation_xml.xpath(VALID_XPATH)
                                print pid + " no broadcast metadata, validation: " + validation_result[0] + " " + testString
                            except Exception, err:
                                print "Exception generated for " + validation_url
                                print Exception, err
            except Exception, err:
                print "Exception generated for " + url
                print Exception, err
