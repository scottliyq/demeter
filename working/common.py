import sys
import getopt


#get instance name from param
def load_param():

    name1 = None 
    name2 = None
    program = None
    argv = sys.argv[1:]
 
    try:
        opts, args = getopt.getopt(argv, "s:e:c:p:")  # 短选项模式
     
    except:
        print("Error")
 
    for opt, arg in opts:
        # start date
        if opt in ['-s']:
            start = arg
        # end date
        if opt in ['-e']:
            end = arg
            #chain id
        if opt in ['-c']:
            chain_id = arg
                # pool id
        if opt in ['-p']:
            pool_id = arg

    return start, end,  chain_id, pool_id