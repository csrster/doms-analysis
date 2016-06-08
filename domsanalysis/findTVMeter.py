#!/usr/bin/python
import sys, config, os, urllib, urllib2
from lxml import etree

MAIN_TITLE_XPATH = "/tvmeter:tvmeterProgram/tvmeter:mainTitle/text()"
STATIONID_XPATH = "/tvmeter:tvmeterProgram/tvmeter:stationID/text()"
STARTDATE_XPATH = "/tvmeter:tvmeterProgram/tvmeter:startDate/text()"
ENDDATE_XPATH = "/tvmeter:tvmeterProgram/tvmeter:endDate/text()"
ORIGINAL_ENTRY_XPATH = "/tvmeter:tvmeterProgram/tvmeter:originalEntry/text()"
NAMESPACES = {"tvmeter": "http://doms.statsbiblioteket.dk/types/gallup_original/0/1/#"}

if __name__ == "__main__":
    config.parseOpts(sys.argv[0:])

    has_tvmeter = open(config.doms_server + "/withTVMeter.txt", "w")
    no_tvmeter = open(config.doms_server + "/withoutTVMeter.txt", "w")

    with open(config.doms_server + "/triples.txt", "r") as uuids:
        count = 0
        for uuid in uuids:
            uuid = uuid.rstrip()
            url = "http://" + config.doms_server + ":" + str(config.doms_port) + "/fedora/objects/" + uuid + "/datastreams/GALLUP_ORIGINAL/content"
            if config.debug:
                print("Opening " + url)
            request = urllib2.Request(url)
            tvmeter_data = urllib2.urlopen(request)
            tvmeter_string = tvmeter_data.read()
            tvmeter_data.close()
            tvmeter_xml = etree.fromstring(tvmeter_string)
            main_title = tvmeter_xml.xpath(MAIN_TITLE_XPATH, namespaces=NAMESPACES)
            if len(main_title) == 0:
                no_tvmeter.write(uuid + "\n")
            else:
                # Output format is tvmeter_line stationID startdate enddate originalEntry
                station_id = tvmeter_xml.xpath(STATIONID_XPATH, namespaces=NAMESPACES)[0]
                startdate = tvmeter_xml.xpath(STARTDATE_XPATH, namespaces=NAMESPACES)[0]
                enddate = tvmeter_xml.xpath(ENDDATE_XPATH, namespaces=NAMESPACES)[0]
                original_entry = tvmeter_xml.xpath(ORIGINAL_ENTRY_XPATH, namespaces=NAMESPACES)[0]
                output_string = "%(tvmeter_line)s,%(station_id)s,%(startdate)s,%(enddate)s,%(original_entry)s\n" % locals()
                has_tvmeter.write(output_string.encode("utf-8"))
            count = count + 1
            if (config.debug and count == 10):
                sys.exit(0)
            if count % 1000 == 0:
                print "Processed " + str(count) + " tvmeter_lines."