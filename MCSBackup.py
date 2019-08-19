import paramiko
import os
import logzero
from logzero import logger
from datetime import date,datetime
import threading
import time
from dateutil.relativedelta import *
import mysql.connector

# Configure
INSTALLDIR='/data/mariadb/columnstore'
BACKUPDIR=''
LOGDIR=''
OAMIP = ''
SSHUSER=''
SSHPASS=''
SSHPORT=22
MCSUSER=''
MCSPASS=''
LOCKUSER = ''
LOCKPASS = ''
ARCHIVEDAY=1
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
_filename = LOGDIR + "/" + str(BACKDAY) + '_mcsbackup.log'
logzero.logfile(_filename,disableStderrLogger=True)  

## Backup Function
def backupEXC(module_nm,target_ip):
    # Declare
    ssh_cli = paramiko.SSHClient()
    ssh_cli.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Connection
    try:
        if not SSHPASS:
            ssh_cli.connect(target_ip,username=SSHUSER,port=SSHPORT,timeout=10)
            logger.info(module_nm + ' Module Connection Success.')
        else:
            ssh_cli.connect(target_ip,username=SSHUSER,password=SSHPASS,port=SSHPORT,timeout=10)
            logger.info(module_nm + ' Module Connection Success.')
    except Exception as MODULEERR:
        logger.error(MODULEERR)

    ###############################################################################
    # BEFORE BACKUP DELETE
    ###############################################################################
    TDAY = date.today()
    YDAY = TDAY + relativedelta(days=-(ARCHIVEDAY))
    DELDAY=YDAY.strftime('%Y%m%d')
    DELTARGET = BACKUPDIR + "/" + str(DELDAY)
    _rmTARGET = """rm -rf %s""" % DELTARGET
    ssh_cli.exec_command(_rmTARGET)

    ###############################################################################
    # CREATE BACKUPBASE DIRECTORY
    ###############################################################################
    BACKUPBASE = BACKUPDIR + "/" + str(BACKDAY)
    _backupDIRCHK = """(ls %s >> /dev/null 2>&1 && echo Y) || echo N""" % BACKUPBASE
    stdin, stdout, stderr = ssh_cli.exec_command(_backupDIRCHK)
    _backupDIRRESULT = stdout.readlines()
    
    
    if _backupDIRRESULT[0][:-1] == "N":
        _mkdir = """/usr/bin/mkdir -p %s""" % BACKUPBASE            
        ssh_cli.exec_command(_mkdir)

        stdin, stdout, stderr = ssh_cli.exec_command(_backupDIRCHK)
        _backupDIRRESULT = stdout.readlines()

        if _backupDIRRESULT[0][:-1] == "Y":
            logger.info(module_nm + " Directory Make Success : " + str(BACKUPBASE))
            _chonwDir = "/usr/bin/chown -R %s:%s %s" % (SSHUSER,SSHUSER,BACKUPBASE)
            ssh_cli.exec_command(_chonwDir)
        else:
            logger.error("Directory Make Failed : " + str(BACKUPBASE))
            exit()
    else:
        _chonwDir = "/usr/bin/chown -R %s:%s %s" % (SSHUSER,SSHUSER,BACKUPBASE)
        ssh_cli.exec_command(_chonwDir)
    ###############################################################################
    # Columnstore Config File Copy
    ###############################################################################
    try:
        _configCOPY = """cp %s/etc/Columnstore.xml %s""" % (INSTALLDIR,BACKUPBASE)
        _myconfigCOPY = """cp %s/mysql/my.cnf %s""" % (INSTALLDIR,BACKUPBASE)
        ssh_cli.exec_command(_configCOPY)
        ssh_cli.exec_command(_myconfigCOPY)
        logger.info(module_nm + " Configure File Copy Success.")
    except Exception as CONERR:
        logger.error(module_nm + " " + CONERR)

    ###############################################################################
    # Data File Copy
    ###############################################################################
    # CREATE DBROOT BACKUPDIR 
    DBROOTBACKUPDIR = BACKUPBASE + "/" + module_nm + "_dbroot"
    _dbrootMAKE = """/usr/bin/mkdir -p %s""" % DBROOTBACKUPDIR   
    ssh_cli.exec_command(_dbrootMAKE)
    logger.info(module_nm + " DBROOT Backup DIR Create Success.")

    _checkCOM = """du -sh %s | awk '{printf $1}'"""
    for idx,DBROOTPATH in enumerate(MCSDBROOT):        
        
        # DBROOT BACKUP(RSYNC)
        DATAORGDIR = INSTALLDIR + "/" + "data" + str(idx+1)
        DATADIR = DATAORGDIR + "/" + "000.dir"  
        _dataDIRCHK= """(ls %s >> /dev/null 2>&1 && echo Y) || echo N""" % DATADIR
        stdin, stdout, stderr = ssh_cli.exec_command(_dataDIRCHK)
        _DATARESULT = stdout.readlines()       

        if _DATARESULT[0][:-1] == "Y":
            logger.info(module_nm + " DBROOT Backup Start.")
            _backupEXE = """rsync -a --delete %s %s""" % (DATAORGDIR,DBROOTBACKUPDIR)
            ssh_cli.exec_command(_backupEXE)
            
            while True:
                # Check Command
                _checkORG = _checkCOM % DATAORGDIR
                _backupPATH = DBROOTBACKUPDIR + "/data" + str(idx+1)
                _checkBAKCUP = _checkCOM % _backupPATH
                stdin, stdout, stderr = ssh_cli.exec_command(_checkORG)
                _orgRESULT = stdout.readlines()
                stdin, stdout, stderr = ssh_cli.exec_command(_checkBAKCUP)
                _backRESULT = stdout.readlines()

                if _orgRESULT[0] == _backRESULT[0]:
                    logger.info(module_nm + " DBROOT Backup Success.")
                    break
                else:
                    time.sleep(5)            

    # CREATE INNODB BACKUP DIR
    INNODBBACKUPDIR = BACKUPBASE + "/" + module_nm + "_innodb"
    _innodbMAKE = """/usr/bin/mkdir -p %s""" % INNODBBACKUPDIR   
    ssh_cli.exec_command(_innodbMAKE)
    logger.info(module_nm + " InnoDB Backup DIR Create Success.")

    # InnoDB BACKUP
    innodbDIR = INSTALLDIR + "/mysql/db"
    logger.info(module_nm + " InnoDB Backup Start.")
    _innobackupEXE = """rsync -a --delete %s %s""" % (innodbDIR,INNODBBACKUPDIR)
    ssh_cli.exec_command(_innobackupEXE)
    time.sleep(2)
    while True:
        # Check Command
        _checkINNOORG = _checkCOM % innodbDIR
        _checkINNOBAKCUP = _checkCOM % INNODBBACKUPDIR
        stdin, stdout, stderr = ssh_cli.exec_command(_checkINNOORG)
        _orgINNORESULT = stdout.readlines()
        stdin, stdout, stderr = ssh_cli.exec_command(_checkINNOBAKCUP)
        _backINNORESULT = stdout.readlines()

        if _orgINNORESULT[0] == _backINNORESULT[0]:
            logger.info(module_nm + " InnoDB Backup Success.")
            break
        else:
            time.sleep(5) 

    ###############################################################################
    # Version File Copy
    ###############################################################################
    try:       
        _versionCopy = """cp %s/releasenum %s""" % (INSTALLDIR,BACKUPBASE)
        ssh_cli.exec_command(_versionCopy)
        logger.info(module_nm + " Version File Copy.")
    except Exception as VERERR:
        logger.error(module_nm + " " + VERERR)

    # Connection Close
    ssh.close()



###############################################################################
# Main
###############################################################################
try:
    if not SSHPASS:
        ssh.connect(OAMIP,username=SSHUSER,port=SSHPORT,timeout=10)
        logger.info('OAM Connection Success.')
    else:
        ssh.connect(OAMIP,username=SSHUSER,password=SSHPASS,port=SSHPORT,timeout=10)
        logger.info('OAM Connection Success.')
    
    # Check System Status
    _CHKSTAT = """%s/bin/mcsadmin getSystemStatus  | tail -n +9  | sed '/^$/d' | grep 'System' | awk '{ printf $2 }'""" % INSTALLDIR    
    stdin, stdout, stderr = ssh.exec_command(_CHKSTAT)
    _sshdata = stdout.readlines()
    MCSSTATUS = _sshdata[0]
    
    # Main Execusion
    if MCSSTATUS == "ACTIVE":
        logger.info('MCS Status : ' + str(MCSSTATUS))
        _CHKIP = """%s/bin/mcsadmin getSystemnetworkconfig  | tail -n +7 | sed '/^$/d' | awk '{ printf $1":"$7 " "; }'""" % INSTALLDIR
        stdin, stdout, stderr = ssh.exec_command(_CHKIP)
        _sshdata = stdout.readlines()
        _sshdata = _sshdata[0]
        _sshdata_con = _sshdata.split(" ") 

        ###############################################################################
        # MODULE INFO
        ###############################################################################
        MCSSVR=[]
        for _svr in _sshdata_con:
            if _svr:
                x = _svr.split(":") 
                MCSSVR.append({"module":x[0],"ip":x[1]})
        logger.info("MCS Module Info : "+ str(MCSSVR))
        logger.info("MCS PM Count : "+ str(len(MCSSVR)))

        ###############################################################################
        # DBROOT INFO
        ###############################################################################
        MCSDBROOT = []
        ## DBROOT COUNT
        _DBROOT="""xmllint --xpath "string(//DBRootCount)" %s/etc/Columnstore.xml""" % INSTALLDIR         
        stdin, stdout, stderr = ssh.exec_command(_DBROOT)
        _sshdata = stdout.readlines()
        logger.info("DBROOT Count : " + str(_sshdata[0]))

        ## DBROOT PATH
        for _DBROOTID in range(1,int(_sshdata[0])+1):
            _DBROOTPATH="""xmllint --xpath "string(//DBRoot%s)" %s/etc/Columnstore.xml""" % (_DBROOTID,INSTALLDIR)
            stdin, stdout, stderr = ssh.exec_command(_DBROOTPATH)
            _DBROOTPATHDATA = stdout.readlines()
            MCSDBROOT.append(_DBROOTPATHDATA[0]) 
        logger.info("DBROOT PATH : " + str(MCSDBROOT))

        ###############################################################################
        # suspendDBWrites(OAM)
        ###############################################################################        
        _SUSPENDWRITE = """%s/bin/mcsadmin suspendDatabaseWrites y""" % INSTALLDIR
        stdin, stdout, stderr = ssh.exec_command(_SUSPENDWRITE)
        _sshdata = stdout.readlines()
        _SUSPENDRESULT = _sshdata[4].split(" ")
        
        if len(_SUSPENDRESULT) > 7 and _SUSPENDRESULT[7][:-1] == "completed":
            logger.info("Suspend DB Write Complete.")
        else:
            logger.error("Suspend DB Write Error.")   

        # OAM Session Close
        ssh.close() 

        ###############################################################################
        # InnoDB Global Read Lock(OAM)
        ############################################################################### 
        logger.info("MCS InnoDB Global Read Lock.")
        _dbConfig = {'user':LOCKUSER,'password':LOCKPASS,'host':OAMIP,'port':3306,'connection_timeout':10}
        _dbCon = mysql.connector.connect(**_dbConfig)
        _dbCur = _dbCon.cursor(dictionary=True)
        _dbCur.execute("FLUSH TABLES WITH READ LOCK")
        time.sleep(10)

        ###############################################################################       
        # Backup Execusion
        ###############################################################################
        for idx,DIVSVR in enumerate(MCSSVR):
            globals()[DIVSVR['module']] = threading.Thread(target=backupEXC,args=tuple(DIVSVR.values()))          
            globals()[DIVSVR['module']].start()           
        
        for idx,DIVSVR in enumerate(MCSSVR):
             globals()[DIVSVR['module']].join()

        ###############################################################################
        # OAM Reconnection
        ###############################################################################
        try:
            if not SSHPASS:
                ssh.connect(OAMIP,username=SSHUSER,port=SSHPORT,timeout=10)
                logger.info('OAM Connection Success.')
            else:
                ssh.connect(OAMIP,username=SSHUSER,password=SSHPASS,port=SSHPORT,timeout=10)
                logger.info('OAM Connection Success.')             
        except Exception as MODULEERR:
            logger.error(MODULEERR)

        ###############################################################################
        # INNODB BACKUP
        ###############################################################################
        DUMPFILE = BACKUPDIR + "/" + str(BACKDAY) + "/innodb_data_dump.sql"
        _dumpSQL = """%s/mysql/bin/mysqldump -u%s -p'%s' -e -R --all-databases > %s""" % (INSTALLDIR,MCSUSER,MCSPASS,DUMPFILE)
        #_dumpSQL = """%s/mysql/bin/mysqldump -u%s -p'%s' -e -R --databases isestat mysql > %s""" % (INSTALLDIR,MCSUSER,MCSPASS,DUMPFILE)
        ssh.exec_command(_dumpSQL)

        while True:
            _checkDUMP = """%s/mysql/bin/mysql -u%s -p'%s'""" % (INSTALLDIR,MCSUSER,MCSPASS)
            _checkQuery = """ -srN -e'select COUNT(1) from information_schema.processlist where INFO like "%SQL_NO_CACHE%"'"""
            _command = _checkDUMP + _checkQuery
            stdin, stdout, stderr = ssh.exec_command(_command)
            _checkDUMPDATA = stdout.readlines()
            if int(_checkDUMPDATA[0][:-1]) > 1:
                time.sleep(5)
            else:
                logger.info('OAM InnoDB Data Dump Success.')
                break

        ###############################################################################
        # resumeDBWrites
        ###############################################################################
        _RESUMEWRITE = """%s/bin/mcsadmin resumeDatabaseWrites y""" % INSTALLDIR
        stdin, stdout, stderr = ssh.exec_command(_RESUMEWRITE)
        _sshdata = stdout.readlines()
        _RESUMERESULT = _sshdata[2].split(" ")

        if len(_RESUMERESULT) > 7 and _RESUMERESULT[7][:-1] == "completed":
            logger.info("Resume DB Write Complete.")
        else:
            logger.error("Resume DB Write Error.")

        ssh.close()

        ###############################################################################
        # UNLOCK TABLE
        ###############################################################################
        logger.info("MCS InnoDB Unlock Tables.")
        _dbCur.execute("unlock tables;")
        _dbCur.close()
        _dbCon.close()
    else:
        logger.error('MCS System Not Activate.')
        # Session Close
        ssh.close()       
except Exception as OAMERR:
    logger.error(OAMERR)
