#!/usr/bin/python
import sys, config, os, urllib, urllib2, re, HTMLParser
from lxml import etree

METADATA_XPATH="/ritzau:ritzau_original/text()"
ID_REGEX = re.compile("Id=([0-9]+)")
KANAL_ID_REGEX = re.compile("kanalId=([0-9]+)")
CHANNEL_NAME_REGEX = re.compile("channel_name='([a-zA-Z0-9]*)',")
STARTTID_REGEX = re.compile("starttid=([0-9-:.\s]*)")
SLUTTID_REGEX = re.compile("sluttid=([0-9-:.\s]*)")
NAMESPACES = {"ritzau": "http://doms.statsbiblioteket.dk/types/ritzau_original/0/1/#"}



if __name__ == "__main__":
    config.parseOpts(sys.argv[0:])

    has_ritzau = open(config.doms_server + "/withRitzau.txt", "w")
    no_ritzau = open(config.doms_server + "/withoutRitzau.txt", "w")

    with open(config.doms_server + "/triples.txt", "r") as uuids:
        count = 0
        for uuid in uuids:
            uuid = uuid.rstrip()
            url = "http://" + config.doms_server + ":" + str(config.doms_port) + "/fedora/objects/" + uuid + "/datastreams/RITZAU_ORIGINAL/content"
            request = urllib2.Request(url)
            ritzau_data = urllib2.urlopen(request)
            ritzau_string =ritzau_data.read()
            ritzau_xml = etree.fromstring(ritzau_string)
            ritzau_string = ritzau_xml.xpath(METADATA_XPATH, namespaces=NAMESPACES)[0]
            try:
                ritzau_string = HTMLParser.HTMLParser().unescape(ritzau_string)
            except:
                print ritzau_string
            ritzau_data.close()
            if ritzau_string.find("kanalId") == -1:
                no_ritzau.write(uuid + "\n")
            else:
                #Output format is tvmeter_line,digitv_id, kanalId, channel_name, starttid, stoptid
                try:
                    id = ID_REGEX.search(ritzau_string).group(1)
                    kanal_id = KANAL_ID_REGEX.search(ritzau_string).group(1)
                    channel_name = CHANNEL_NAME_REGEX.search(ritzau_string).group(1)
                    starttid = STARTTID_REGEX.search(ritzau_string).group(1)
                    sluttid = SLUTTID_REGEX.search(ritzau_string).group(1)
                    output_string = "%(tvmeter_line)s,%(id)s,%(kanal_id)s,%(channel_name)s,%(starttid)s,%(sluttid)s\n" % locals()
                    has_ritzau.write(output_string)
                except:
                    print "Cannot match " + ritzau_string
            count = count + 1
            if (config.debug and count == 10):
                sys.exit(0)
            if count % 1000 == 0:
                print "Processed " + str(count) + " tvmeter_lines."