# MCSBackupRestore
기존 ColumnstoreBackup Tool을 참고하여 작성된 스크립트로 네트워크를 통해 실행하긴 하지만 백업 파일이 각 모듈에 저장되어 복구시에도 각 모듈에서 개별적으로 복구됩니다. 

이는 서버의 네트워크 상황을 개선하기 어렵고 서버내 스토리지의 여유가 있는 경우에 사용하기 위하여 작성되었으며, 아래의 서버 구성에서 개발되고 테스트 되었으므로 적용하기 위해서는 많은 테스트가 필요합니다. 

> 2ea Server / Combined / Data Redundancy


## Backup
### Configure
<pre>
<code>
INSTALLDIR='/data/mariadb/columnstore'     # 컬럼스토어의 기본 경로
BACKUPDIR=''                               # 백업 경로
LOGDIR=''                                  # 로그 경로
OAMIP = ''                                 # OAM IP
SSHUSER=''                                 # SSH 접근 계정(모든서버 동일)
SSHPASS=''                                 # SSH 접근 패스워드 (공백인 경우 SSH KEY를 이용해서 접근합니다.
SSHPORT=22                                 # SSH 접근 PORT
MCSUSER=''                                 # 컬럼 스토어 접속 계정(localhost가 allow된 백업계정)
MCSPASS=''                                 # 컬럼 스토어 접속 패스워드
LOCKUSER=''                                # FLUSH TABLES WITH READ LOCK; 기능을 사용할 계정
LOCKPASS=''                                # PASSWORD
ARCHIVEDAY=1                               # 백업 보존 일수
</code>
</pre>

### Backup Flow
+ GET MODULE INFO (Module Name, IP ADDR)
+ GET DBROOT INFO
+ Suspend Database Write(Columnstore)
+ InnoDB Global Read Lock
+ Main Backup Exe
  + Before Backup Delete (ARCHIVEDAY)
  + CREATE Backup Directory
  + Backup Columnstore Configure (ColumnStore.xml / my.cnf)
  + Columnstore Data File Copy
  + InnoDB Data File Copy
  + Version File Copy
+ MySQLDump for OAM
+ Resume Database Write
+ InnoDB Unlock Tables

### Backup Log
<pre>
<code>

[190819 18:13:20] [INFO] MCS Status : ACTIVE
[190819 18:13:20] [INFO] MCS Module Info : [{'module': 'pm1', 'ip': ''}, {'module': 'pm2', 'ip': ''}]
[190819 18:13:20] [INFO] MCS PM Count : 2
[190819 18:13:20] [INFO] DBROOT Count : 2
[190819 18:13:21] [INFO] DBROOT PATH : ['/data/mariadb/columnstore/data1', '/data/mariadb/columnstore/data2']
[190819 18:13:26] [INFO] Suspend DB Write Complete.
[190819 18:13:26] [INFO] MCS InnoDB Global Read Lock.
[190819 18:13:26] [INFO] pm1 Module Connection Success.
[190819 18:13:26] [INFO] pm2 Module Connection Success.
[190819 18:13:26] [INFO] pm1 Directory Make Success : /backup/20190819
[190819 18:13:26] [INFO] pm2 Directory Make Success : /backup/20190819
[190819 18:13:26] [INFO] pm2 Configure File Copy Success.
[190819 18:13:26] [INFO] pm1 Configure File Copy Success.
[190819 18:13:26] [INFO] pm2 DBROOT Backup DIR Create Success.
[190819 18:13:26] [INFO] pm1 DBROOT Backup DIR Create Success.
[190819 18:13:27] [INFO] pm1 DBROOT Backup Start.
[190819 18:13:27] [INFO] pm2 InnoDB Backup DIR Create Success.
[190819 18:13:27] [INFO] pm2 InnoDB Backup Start.
[190819 18:13:29] [INFO] pm2 InnoDB Backup Success.
[190819 18:13:29] [INFO] pm2 Version File Copy.
[190819 18:13:32] [INFO] pm1 DBROOT Backup Success.
[190819 18:13:32] [INFO] pm1 InnoDB Backup DIR Create Success.
[190819 18:13:32] [INFO] pm1 InnoDB Backup Start.
[190819 18:13:34] [INFO] pm1 InnoDB Backup Success.
[190819 18:13:34] [INFO] pm1 Version File Copy.
[190819 18:13:34] [INFO] OAM Connection Success.
[190819 18:13:35] [INFO] OAM InnoDB Data Dump Success.
[190819 18:13:35] [INFO] Resume DB Write Complete.
[190819 18:13:35] [INFO] MCS InnoDB Unlock Tables.
</code>
</pre>

## Restore
### Run Arguments
<pre>
<code>
python MCSRestore.py [Backup Path] [INSTALLPATH] [OAM IP] [SSHUSER] [SSHPASSWORD]
</code>
</pre>

### Restore Flow
+ System Check(State : MAN_OFFLINE)
+ Data Reset (000.dir / DBRM / InnoDB)
+ Clear Shared Memory(/bin/clearShm)
+ Data File Restore(rsync)
+ Configure Backup&Restore(Columnstore.xml/my.cnf)

백업이 복구된 이후에는 수동으로 시스템을 실행해야하고, 쓰기 제한을 해제해야합니다. 사용 구문은 복구 스크립트의 최하단에 로그로 출력됩니다. 

### Restore Log
<pre>
<code>
> python MCSRestore.py /backup/20190819 /data/mariadb/columnstore OAMIP sshuser ******
[190819 18:15:49] [INFO] OAM Connection Success.
[190819 18:15:51] [INFO] MCS Status : MAN_OFFLINE
[190819 18:15:51] [INFO] MCS Status : MAN_OFFLINE
[190819 18:15:51] [INFO] MCS Module Name : pm1 Module IP ADDR : ***.***.***.***
[190819 18:15:51] [INFO] MCS Module Name : pm2 Module IP ADDR : ***.***.***.***
[190819 18:15:51] [INFO] pm1 Module Connection Success.
[190819 18:15:51] [INFO] pm1 ColumnStore Data Reset.
[190819 18:15:51] [INFO] pm1 InnoDB Data Reset.
[190819 18:15:51] [INFO] pm2 Module Connection Success.
[190819 18:15:51] [INFO] pm2 ColumnStore Data Reset.
[190819 18:15:51] [INFO] pm2 InnoDB Data Reset.
[190819 18:15:51] [INFO] OAM Clear Shared Memory.
[190819 18:15:51] [INFO] pm1 ColumnStore Data Recovery.
[190819 18:15:55] [INFO] pm1 ColumnStore Data Complete.
[190819 18:15:55] [INFO] pm1 InnoDB Data Recovery.
[190819 18:15:58] [INFO] pm1 InnoDB Data Complete.
[190819 18:15:58] [INFO] pm1 MySQLD PID Delete.
[190819 18:15:58] [INFO] pm1 ColumnStore Configure Backup AND Recovery.
[190819 18:15:58] [INFO] pm1 MySQLD Configure Backup AND Recovery.
[190819 18:15:58] [INFO] pm2 ColumnStore Data Recovery.
[190819 18:16:01] [INFO] pm2 ColumnStore Data Complete.
[190819 18:16:01] [INFO] pm2 InnoDB Data Recovery.
[190819 18:16:05] [INFO] pm2 InnoDB Data Complete.
[190819 18:16:05] [INFO] pm2 MySQLD PID Delete.
[190819 18:16:05] [INFO] pm2 ColumnStore Configure Backup AND Recovery.
[190819 18:16:05] [INFO] pm2 MySQLD Configure Backup AND Recovery.
[190819 18:16:05] [INFO] =======================================================
[190819 18:16:05] [INFO]  ColumnStore Restore Success.
[190819 18:16:05] [INFO]  Restore Backup Directory : /backup/20190819
[190819 18:16:05] [INFO]  ColumnStore Install Path : /data/mariadb/columnstore
[190819 18:16:05] [INFO]  Backup Recover Day : 2019-08-19
[190819 18:16:05] [INFO]  Run System Start : mcsadmin startSystem y
[190819 18:16:05] [INFO]  Run System Command : mcsadmin resumeDatabaseWrites y
[190819 18:16:05] [INFO] =======================================================
</code>
</pre>
