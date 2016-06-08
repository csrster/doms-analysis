import sys, config, os, urllib, urllib2, re

def main():
    config.parseOpts(sys.argv[0:])
    try:
        os.mkdir(config.doms_server)
    except OSError:
        pass
    values = {
        'type':'triples',
        'lang':'spo',
        'query':'* <info:fedora/fedora-system:def/model#hasModel> <info:fedora/doms:ContentModel_Program>'
    }
    request = urllib2.Request("http://"+config.doms_server+":"+str(config.doms_port)
                              +"/fedora/risearch", urllib.urlencode(values))
    data = urllib2.urlopen(request)
    uuidre = re.compile("uuid:[0-9a-f\-]*")
    count = 0
    with open(config.doms_server + "/triples.txt", "wb") as output_file:
        for line in data:
            matcher = uuidre.search(line)
            if matcher:
                output_file.write(matcher.group()+"\n")
            count = count + 1
            if (config.debug and count == 10):
                sys.exit(0)


if __name__ == "__main__":
     main()
