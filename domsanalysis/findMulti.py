#!/usr/bin/python
import multiprocessing
import sys, config, os, urllib, urllib2, re, HTMLParser
from lxml import etree

# Constants for tvmeter metadata
import traceback

MAIN_TITLE_XPATH = "/tvmeter:tvmeterProgram/tvmeter:mainTitle/text()"
STATIONID_XPATH = "/tvmeter:tvmeterProgram/tvmeter:stationID/text()"
STARTDATE_XPATH = "/tvmeter:tvmeterProgram/tvmeter:startDate/text()"
ENDDATE_XPATH = "/tvmeter:tvmeterProgram/tvmeter:endDate/text()"
ORIGINAL_ENTRY_XPATH = "/tvmeter:tvmeterProgram/tvmeter:originalEntry/text()"

# Constants for ritzau metadata
METADATA_XPATH="/ritzau:ritzau_original/text()"
ID_REGEX = re.compile("Id=([0-9]+)")
KANAL_ID_REGEX = re.compile("kanalId=([0-9]+)")
CHANNEL_NAME_REGEX = re.compile("channel_name='([a-zA-Z0-9]*)',")
STARTTID_REGEX = re.compile("starttid=([0-9-:.\s]*)")
SLUTTID_REGEX = re.compile("sluttid=([0-9-:.\s]*)")

NAMESPACES = {"tvmeter": "http://doms.statsbiblioteket.dk/types/gallup_original/0/1/#",
              "ritzau": "http://doms.statsbiblioteket.dk/types/ritzau_original/0/1/#"}

def lookupGallupDoms(url, uuid, with_queue, without_queue):
    if config.debug:
        print "Starting lookup of " + url
    request = urllib2.Request(url)
    tvmeter_data = urllib2.urlopen(request)
    tvmeter_string = tvmeter_data.read()
    tvmeter_data.close()
    tvmeter_xml = etree.fromstring(tvmeter_string)
    main_title = tvmeter_xml.xpath(MAIN_TITLE_XPATH, namespaces=NAMESPACES)
    if len(main_title) == 0:
        without_queue.put(uuid + "\n")
    else:
        try:
            # Output format is tvmeter_line stationID startdate enddate originalEntry
            station_id = tvmeter_xml.xpath(STATIONID_XPATH, namespaces=NAMESPACES)[0]
            startdate = tvmeter_xml.xpath(STARTDATE_XPATH, namespaces=NAMESPACES)[0]
            enddate = tvmeter_xml.xpath(ENDDATE_XPATH, namespaces=NAMESPACES)[0]
            original_entry = tvmeter_xml.xpath(ORIGINAL_ENTRY_XPATH, namespaces=NAMESPACES)[0]
            output_string = "%(tvmeter_line)s,%(station_id)s,%(startdate)s,%(enddate)s,%(original_entry)s\n" % locals()
            with_queue.put(output_string.encode("utf-8"))
        except:
            # print "Error parsing tvmeter xml for " + tvmeter_line + ":\n" + tvmeter_data + "\n"
            without_queue.put(uuid + "\n")

def lookupRitzauDoms(url, uuid, with_queue, without_queue):
    if config.debug:
        print "Starting lookup of " + url
    request = urllib2.Request(url)
    ritzau_data = urllib2.urlopen(request)
    try:
        ritzau_string =ritzau_data.read()
        ritzau_xml = etree.fromstring(ritzau_string)
        ritzau_string = ritzau_xml.xpath(METADATA_XPATH, namespaces=NAMESPACES)[0]
        ritzau_string = HTMLParser.HTMLParser().unescape(ritzau_string)
        ritzau_data.close()
        if ritzau_string.find("kanalId") == -1:
            without_queue.put(uuid + "\n")
        else:
            #Output format is tvmeter_line,digitv_id, kanalId, channel_name, starttid, stoptid
            id = ID_REGEX.search(ritzau_string).group(1)
            kanal_id = KANAL_ID_REGEX.search(ritzau_string).group(1)
            channel_name = CHANNEL_NAME_REGEX.search(ritzau_string).group(1)
            starttid = STARTTID_REGEX.search(ritzau_string).group(1)
            sluttid = SLUTTID_REGEX.search(ritzau_string).group(1)
            output_string = "%(tvmeter_line)s,%(id)s,%(kanal_id)s,%(channel_name)s,%(starttid)s,%(sluttid)s\n" % locals()
            with_queue.put(output_string)
    except:
        print "Error parsing ritzau data for " + uuid + "\n"
        without_queue.put(uuid + "\n")

def file_append_listen(q, filename):
    with open(filename, 'wb') as file:
        while 1:
            m = q.get()
            if str(m) == 'kill':
                break
            file.write(str(m))
            file.flush()


if __name__ == "__main__":
    config.parseOpts(sys.argv[0:])

    poolsize = int(config.doms_poolsize) + 4;
    manager = multiprocessing.Manager()
    pool = multiprocessing.Pool(poolsize)

    with_tvmeter_file = config.doms_server + "/withTVMeter.txt"
    without_tvmeter_file =config.doms_server + "/withoutTVMeter.txt"
    with_ritzau_file = config.doms_server + "/withRitzau.txt"
    without_ritzau_file =config.doms_server + "/withoutRitzau.txt"


    with_tvmeter_queue = manager.Queue()
    without_tvmeter_queue = manager.Queue()
    with_ritzau_queue = manager.Queue()
    without_ritzau_queue = manager.Queue()

    pool.apply_async(file_append_listen, (with_tvmeter_queue, with_tvmeter_file))
    pool.apply_async(file_append_listen, (without_tvmeter_queue, without_tvmeter_file))
    pool.apply_async(file_append_listen, (with_ritzau_queue, with_ritzau_file))
    pool.apply_async(file_append_listen, (without_ritzau_queue, without_ritzau_file))

    jobs = []
    with open(config.doms_server + "/triples.txt", "r") as uuids:
        count = 0
        for uuid in uuids:
            uuid = uuid.rstrip()
            gallup_url = "http://" + config.doms_server + ":" + str(config.doms_port) + "/fedora/objects/" + uuid + "/datastreams/GALLUP_ORIGINAL/content"
            ritzau_url = "http://" + config.doms_server + ":" + str(config.doms_port) + "/fedora/objects/" + uuid + "/datastreams/RITZAU_ORIGINAL/content"
            if (count % 1000 == 0 and config.debug):
                print("Queuing " + gallup_url + "\n and " + ritzau_url)
            job = pool.apply_async(lookupGallupDoms, (gallup_url, uuid, with_tvmeter_queue, without_tvmeter_queue))
            jobs.append(job)
            job = pool.apply_async(lookupRitzauDoms, (ritzau_url, uuid, with_ritzau_queue, without_ritzau_queue))
            jobs.append(job)
            count = count + 1
            if (config.debug and count == 4000):
                break
            if (count % 1000 == 0):
                print("Queued tvmeter_line nr. " + str(count) + " " + uuid)
    print "Queued " + str(len(jobs)) + " jobs. (2 pr. tvmeter_line)"
    jcount = 0
    for job in jobs:
        try:
            job.get()
        except:
            traceback.print_exc(limit=20)
        jcount = jcount + 1
        if (jcount % 1000 == 0):
            print("Got job number " + str(jcount) + ". \n")
    with_tvmeter_queue.put('kill')
    without_tvmeter_queue.put('kill')
    with_ritzau_queue.put('kill')
    without_ritzau_queue.put('kill')
    pool.close()