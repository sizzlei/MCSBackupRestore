import paramiko
import os
import logzero
from logzero import logger
from datetime import date,datetime
import time
from dateutil.relativedelta import *
import sys

# Days 
TDAY = date.today()
BACKDAY=TDAY.strftime('%Y%m%d')

# Declare
## SSH
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

## Logger
log_format = '%(color)s[%(asctime)s] [%(levelname)s]%(end_color)s %(message)s'
formatter = logzero.LogFormatter(fmt=log_format)
logzero.setup_default_logger(formatter=formatter)
logzero.formatter(formatter=formatter)

if len(sys.argv) < 2:
    print("Not Argv Set")    
    print("Desc : python MCSRestore.py [Backup Path] [INSTALLPATH] [OAM IP] [SSHUSER] [SSHPASSWORD] ")
    exit()
else:
    try:
        BACKUPPATH = sys.argv[1]
        INSTALLPATH = sys.argv[2]
        OAMIP = sys.argv[3]
        SSHUSER = sys.argv[4]
        SSHPASS = sys.argv[5] 
        SSHPORT = 22    

        if not BACKUPPATH or not SSHUSER or not SSHPASS or not INSTALLPATH:
            print("Not Argv Set")    
            print("Desc : python MCSRestore.py [Backup Path] [INSTALLPATH] [OAM IP] [SSHUSER] [SSHPASSWORD] ")
            exit()
    except Exception as ARGERR:
        print("Not Argv Set")    
        print("Desc : python MCSRestore.py [Backup Path] [INSTALLPATH] [OAM IP] [SSHUSER] [SSHPASSWORD] ")
        exit()


###############################################################################
# MCS SHUTDOWN System Shutdown
###############################################################################
# OAM Connection
try:
    ssh.connect(OAMIP,username=SSHUSER,password=SSHPASS,port=SSHPORT,timeout=10)
    logger.info('OAM Connection Success.')

    # Check System Status
    _CHKSTAT = """%s/bin/mcsadmin getSystemStatus  | tail -n +9  | sed '/^$/d' | grep 'System' | awk '{ printf $2 }'""" % INSTALLPATH    
    stdin, stdout, stderr = ssh.exec_command(_CHKSTAT)
    _sshdata = stdout.readlines()
    MCSSTATUS = _sshdata[0]

    # System Shutdown Check
    if MCSSTATUS == "MAN_OFFLINE":
        logger.info('MCS Status : ' + str(MCSSTATUS))
        logger.info('MCS Status : ' + str(MCSSTATUS))
        ###############################################################################
        # MODULE INFO
        ###############################################################################
        _CHKIP = """%s/bin/mcsadmin getSystemnetworkconfig  | tail -n +7 | sed '/^$/d' | awk '{ printf $1":"$7 " "; }'""" % INSTALLPATH
        stdin, stdout, stderr = ssh.exec_command(_CHKIP)
        _sshdata = stdout.readlines()
        _sshdata = _sshdata[0]
        _sshdata_con = _sshdata.split(" ") 
        
        MCSSVR=[]
        for _svr in _sshdata_con:
            if _svr:
                x = _svr.split(":") 
                MCSSVR.append({"module":x[0],"ip":x[1]})
                logger.info("MCS Module Name : "+ str(x[0]) + " Module IP ADDR : " + str(x[1]))

        ###############################################################################
        # Data Reset
        ###############################################################################    
        for _svrinfo in MCSSVR:
            try:
                ssh_svr = paramiko.SSHClient()
                ssh_svr.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_svr.connect(_svrinfo['ip'],username=SSHUSER,password=SSHPASS,port=SSHPORT,timeout=10)
                logger.info(_svrinfo['module'] + " Module Connection Success.")



                logger.info(_svrinfo['module'] + " ColumnStore Data Reset.")
                _rmData = """rm -rf %s/data*/000.dir""" % INSTALLPATH
                ssh_svr.exec_command(_rmData)
                _rmDBRM = """rm -rf %s/data1/systemFile/dbrm/* """ % INSTALLPATH
                ssh_svr.exec_command(_rmDBRM)

                logger.info(_svrinfo['module'] + " InnoDB Data Reset.")
                _rmINNODB = """rm -rf %s/mysql/db/* """ % INSTALLPATH                
                ssh_svr.close()
            except Exception as MODULEERR:
                logger.error(MODULEERR)
                exit()
        ###############################################################################
        # Clear Shared Memory(OAM)
        ###############################################################################    
        logger.info("OAM Clear Shared Memory.")
        _clearSHM = """%s/bin/clearShm """ % INSTALLPATH        
        ssh.exec_command(_clearSHM)

        ###############################################################################
        # Data File Recovery
        ###############################################################################   
        _rsyncCom = """rsync -a --delete %s %s"""
        for _svrinfo in MCSSVR:
            try:
                ssh_svr = paramiko.SSHClient()
                ssh_svr.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_svr.connect(_svrinfo['ip'],username=SSHUSER,password=SSHPASS,port=SSHPORT,timeout=10)
                # Columnstore Recovery
                logger.info(_svrinfo['module'] + " ColumnStore Data Recovery.")
                _dataBack = BACKUPPATH + "/pm*_dbroot/data*" 
                _ORGPATH = INSTALLPATH + "/"
                _colRecover = _rsyncCom % (_dataBack,_ORGPATH)
                ssh_svr.exec_command(_colRecover)

                while True:
                    _chkPS = """ps -ef |grep rsync | wc -l"""
                    stdin, stdout, stderr = ssh_svr.exec_command(_chkPS)
                    _sshdata = stdout.readlines()
                    if int(_sshdata[0][:-1]) > 2:
                        time.sleep(3)
                    else:
                        logger.info(_svrinfo['module'] + " ColumnStore Data Complete.")
                        break

                # INNODB Recovery
                logger.info(_svrinfo['module'] + " InnoDB Data Recovery.")
                _innoBack = BACKUPPATH + "/pm*_innodb/db"
                _ORGINNOPATH = INSTALLPATH + "/mysql/"
                _innoRecover = _rsyncCom % (_innoBack,_ORGINNOPATH)
                ssh_svr.exec_command(_innoRecover)

                while True:
                    _chkPS = """ps -ef |grep rsync | wc -l"""
                    stdin, stdout, stderr = ssh_svr.exec_command(_chkPS)
                    _sshdata = stdout.readlines()

                    if int(_sshdata[0][:-1]) > 2:
                        time.sleep(3)
                    else:
                        logger.info(_svrinfo['module'] + " InnoDB Data Complete.")
                        break
                
                # MYSQLD PID DELETE
                logger.info(_svrinfo['module'] + " MySQLD PID Delete.")
                _rmPID = """rm -rf %s/mysql/db/*.pid""" % INSTALLPATH
                ssh_svr.exec_command(_rmPID)


                ###############################################################################
                # Configure Recovery
                ###############################################################################
                _mvColCon = """mv %s/etc/Columnstore.xml %s/etc/Columnstore.xml_back_%s""" % (INSTALLPATH,INSTALLPATH,BACKDAY)
                ssh_svr.exec_command(_mvColCon)
                _cpColCon = """cp %s/Columnstore.xml %s/etc/Columnstore.xml""" % (BACKUPPATH,INSTALLPATH)
                ssh_svr.exec_command(_cpColCon)
                logger.info(_svrinfo['module'] + " ColumnStore Configure Backup AND Recovery.")

                _mvMyCon = """mv %s/mysql/my.cnf %s/mysql/my.cnf_back_%s""" % (INSTALLPATH,INSTALLPATH,BACKDAY)
                ssh_svr.exec_command(_mvMyCon)
                _cpMyCon = """cp %s/my.cnf %s/mysql/my.cnf""" % (BACKUPPATH,INSTALLPATH)
                ssh_svr.exec_command(_cpMyCon)
                logger.info(_svrinfo['module'] + " MySQLD Configure Backup AND Recovery.")

                ssh_svr.close()
            except Exception as MODULEERR:
                logger.error(MODULEERR)
                exit()
        logger.info("=======================================================")
        logger.info(" ColumnStore Restore Success." )
        logger.info(" Restore Backup Directory : " + str(BACKUPPATH))
        logger.info(" ColumnStore Install Path : " + str(INSTALLPATH))
        logger.info(" Backup Recover Day : " + str(TDAY))
        logger.info(" Run System Start : mcsadmin startSystem y")
        logger.info(" Run System Command : mcsadmin resumeDatabaseWrites y")
        logger.info("=======================================================")
    else:
        logger.error("System didn't Shutdown. ")
        logger.error("Run Command : mcsadmin shutdownSystem y")               
    
    ssh.close()

except Exception as MAINERR:
    logger.error(MAINERR)
