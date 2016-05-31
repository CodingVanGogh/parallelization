



from pmap import pmapp




if __name__ == "__main__":
    def do_it(a, b=8, c=10):
        time.sleep(1)
        return a + b  + c

    # args
    args = [ (_,) for _ in xrange(10) ]
    kwargs = []
    func = do_it
    threads = 8

    # user input ends here
    print "Start time : ", datetime.datetime.now()
    error, output = pmapp(func, args, kwargs, threads, default_output=None)
    print "End time : " , datetime.datetime.now()
    print "Errors : "
    pprint(error)
    print "Output : "
    print(output)
