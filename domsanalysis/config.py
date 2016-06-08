import sys, getopt, urllib2

doms_server = "alhena.statsbiblioteket.dk"
doms_port = 7980
doms_username = "fedoraReadOnlyAdmin"
#doms_password = "2ZeMA1bN"
doms_poolsize= 10
debug = False;

def doAuth():
    top_level_url='http://'+doms_server+":"+str(doms_port)
    password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
    password_mgr.add_password(None, top_level_url, doms_username, doms_password)
    handler = urllib2.HTTPBasicAuthHandler(password_mgr)
    opener = urllib2.build_opener(handler)
    urllib2.install_opener(opener)

def usage(argv):
    print "Usage: " + argv[0] + " -d <doms_server> -p <doms_port> -u <doms_username> -P <doms_password> -s <doms_poolsize>"


def parseOpts(argv):
    global doms_server
    global doms_port
    global doms_username
    global doms_password
    global doms_poolsize
    global debug
    opts, args =  getopt.getopt(argv[1:], "hd:p:u:P:s:", ["debug"])
    for opt, arg in opts:
        if opt == "-h":
            usage(argv)
            sys.exit()
        elif opt == "-d":
            doms_server = arg
        elif opt == "-p":
            doms_port = arg
        elif opt == "-u":
            doms_username = arg
        elif opt == "-P":
            doms_password = arg
        elif opt == "-s":
            doms_poolsize = arg
        elif opt == "--debug":
            debug = True
        else:
            usage(argv)
            sys.exit(2)
    try:
        doAuth()
    except:
        usage(argv)
        sys.exit(2)


if __name__ == "__main__":
    parseOpts(sys.argv[0:])
