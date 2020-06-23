# Encircling-net

Used to download company's data from Gsxt_Mobile or Gsxt_PC

## Usage

```
supervisord's ENV_HOME should be /home/developer

Mode1: download company baseinfi and reginfo from Gsxt_Mobile and write them into MySQL
        modify GSXT_MOBILE_DISCOVER_config.ini
        
        [API]
        PROXY_API: the url get proxy
        
        [PARM]
        EYEAR: the param for GSXT_MOBILE to search 1 means the latest year and 2 means the latest 2 to 5
                years
        AREA: the param for GSXT_MOBILE to search
        
        [COHERSOUP]
        DIR: the dir for COHERSOUP/cohersoup
        
        [LOG]
        DIR: the log dir for downloader
        MYSQL_SEARCH_PIPLINE_LOG_DIR: the log dir for mysql_pipline
        
        
        [DOWNLOADER]
        PROCESS_NUM: the num of process when use multiprocessing to init dawg list
        DAWG_LOADER_LIMIT: the max num of dawg case when use multiprocessing to init dawg list
        CUT_THRESHOLD: only when the num of companies whose name startwith the pre more than this num,
                       the pre will build sub_branch
        NUMBER = 0  count the num of companies downoader downloading, keep the default
        THRESHOLD: the most num of companies can the downloader download at the same time
        THRESHOLD_429: if the percentage of 429(resp_coed) bigger than this rate, won't use abuyun and change to use 
                       normal http proxy
        THRESHOLD_403: if the percentage of 403(resp_coed) bigger than this rate, won't use abuyun and change to use 
                       normal http proxy
        SEM_NUM: the max number of requests when use asyncio
        UA: default useragent 
            
        [REDIS_TAG]
        REDIS_DAWG_LIST_NAME: the name of redis_list which store dawg_name
        REDIS_TAG_HOST: redis server host
        REDIS_TAG_PORT: redis server port
        REDIS_TAG_DB: redis DB
        REDIS_TAG_EX: the expiration time of tag which judge use abuyun or normal http proxy
        
        
        [KAFKA]
        KAFKA_TOPIC: the topic of kafka which store resp.text when find new_company
        KAFKA_BOOTSTRAP_SERVERS: the server host of kafka
        KAFKA_GROUP: the group name of comsumer
        
        
        [MONGO]
        MONGODB_PORT: the mongo server port which store resp.text
        MONGODB_SERVER: the mongo server host which store resp.text
        MONGODB_DB: the mongo db name which store resp.text
        MONGODB_COLLECTION: the mongo server collection name which store resp.text
        MONGODB_USER: mongo user name
        MONGODB_PASSWORD: mongo password
        MONGODB_COLLECTIONS: mysql_pipline get data from these mongo collections and write them into mysql
                            ([{"host": ,"port": ,"db": ,"collection": ,"user": ,"password": }])
        
        
        [MYSQL_PIPLINE]
        TIMES: the time of parser run one round
        EVERYTIME: the number of record parse a time 
        DISCOVER_EVERYTIME: the number of companies write into mysql a time
        PROCESSING_NUMBER: the number of process when use multiprocessing for parser 
        QUEUE_NUMBER: the length of queue which store the parse result
        
       cd tools
       python GSXT_MOBILE_DISCOVER_SCRIPT.py DISCOVER GSXT_MOBILE_DISCOVER_config.ini
       python GSXT_MOBILE_PIPLINE_SCRIPT.py SEARCH GSXT_MOBILE_DISCOVER_config.ini
       python GSXT_MOBILE_PIPLINE_SCRIPT.py SEARCH_MYSQL GSXT_MOBILE_DISCOVER_config.ini

Mode2: update or replenish all information except annual report from Gsxt_Mobile and write them into MySQL
        modify GSXT_MOBILE_DETAIL_config.ini
        
        [API]
        DETAIL_COMPANY_URL: from this url get creditcode to download detail
        PROXY_API: from this url get normal http proxy
        
        
        [LOG]
        DIR: the log dir for downloader
        MYSQL_PIPLINE_LOG_DIR: the log dir for mysql_pipline
        
        
        [COHERSOUP]
        DIR: the dir for COHERSOUP/cohersoup
        
        
        [DOWNLOADER]
         NUMBER = 0  count the num of companies downoader downloading, keep the default
         THRESHOLD: the most num of companies can the downloader download at the same time
         THRESHOLD_429: if the percentage of 429(resp_coed) bigger than this rate, won't use abuyun and change to use 
                       normal http proxy
         THRESHOLD_403: if the percentage of 403(resp_coed) bigger than this rate, won't use abuyun and change to use 
                       normal http proxy
         SEM_NUM: the max number of requests when use asyncio
         UA: default useragent 
        
        [DETAIL_NEED] : the downloader decide which message to download from this tag when download normal detail
        annual = False
        baseinfo = True
        shareholder = True
        employe = True
        branch = True
        nerecitempub = True
        liquidation = True
        changerecord = True
        mortreginfo = True
        equitypledge = True
        trademark = True
        checkinformation = True
        drraninsres = True
        administrativelicensing = True
        administrativepenalty = True
        exceptioninformation = True
        judicialinformation = True
        illegalinformation = True
        self_administrativelicensing = True
        self_shareholder = True
        self_insalterstockinfo = True
        self_propledgereginfo = True
        self_appsimplecancelobjection = True
        self_epubgroupmenberinfo = True
        self_abolishmentlicenseinfo = True
        
        
        [KAFKA]
        KAFKA_TOPIC: the kafka topic which save resp.text when download detail
        KAFKA_BOOTSTRAP_SERVERS: the kafka server which save resp.text when download detail
        KAFKA_GROUP = the group name of consumer
        
        [REDIS_TAG]
        REDIS_TAG_HOST: redis server
        REDIS_TAG_PORT: redis port 
        REDIS_TAG_DB: redis db
        REDIS_TAG_EX: the expiration time of tag which judge use abuyun or normal http proxy
        
        
        [MONGO]
        MONGODB_SERVER: the mongo server which save detail resp.text
        MONGODB_PORT: the mongo port
        MONGODB_DB: th emongo db name which store detail resp.text
        MONGODB_COLLECTION: the mongo collection which sore detail resp.text
        MONGODB_USER: the user name of mongodb
        MONGODB_PASSWORD: the password of mongo db
        MONGODB_COLLECTIONS = [{"host": mysql_pipline get data to write in mysql from this mongo server,
                                "port": the mongo port,
                                "db": name of mongo db,
                                "collection": name of mongo collection,
                                "user": name of mongo user,
                                "password": password of mongodb
                              }
                              ]
        
        
        [MYSQL_PIPLINE]
        TIMES: the time of parser run one round
        EVERYTIME:  the number of record parse a time 
        DETAIL_EVERYTIME: the num of companies write in mysql one time
        PROCESSING_NUMBER: the number of process which run pipline
        QUEUE_NUMBER: whe length of queue which store parse result
        
        
        [UPDATE_GENERATOR]
        QUEUE_SIZE: the length of queue in generator which store creditcode
        ADD_TO_QUEUE_EVERYTIME: the num of creditcodes push into queue every time
        UPDATE_SRART_ID_TAG: the tag mark the company which is the last company to update last time

        
        cd tools
        python GSXT_MOBILE_GENERATOR_SCRIPT.py UPDATE GSXT_MOBILE_DETAIL_config.ini
        python GSXT_MOBILE_DISCOVER_SCRIPT.py DETAIL GSXT_MOBILE_DETAIL_config.ini
        python GSXT_MOBILE_PIPLINE_SCRIPT.py DETAIL GSXT_MOBILE_DETAIL_config.ini
        python GSXT_MOBILE_PIPLINE_SCRIPT.py DETAIL_MYSQL GSXT_MOBILE_DETAIL_config.ini

Mode3: update or replenish annual and write them into MySQL
         modify  GSXT_MOBILE_ANNUAL_config.ini
         
         [API]
        DETAIL_COMPANY_URL: from this url get creditcode to download detail
        PROXY_API: from this url get normal http proxy
        
        
        [LOG]
        DIR: the log dir for downloader
        MYSQL_PIPLINE_LOG_DIR: the log dir for mysql_pipline
        
        
        [COHERSOUP]
        DIR: the dir for COHERSOUP/cohersoup
        
        
        [DOWNLOADER]
         NUMBER = 0  count the num of companies downoader downloading, keep the default
         THRESHOLD: the most num of companies can the downloader download at the same time
         THRESHOLD_429: if the percentage of 429(resp_coed) bigger than this rate, won't use abuyun and change to use 
                       normal http proxy
         THRESHOLD_403: if the percentage of 403(resp_coed) bigger than this rate, won't use abuyun and change to use 
                       normal http proxy
         SEM_NUM: the max number of requests when use asyncio
         UA: default useragent 
        
        [ANNUAL_NEED] : the downloader decide which message to download from this tag when download normal detail
        annual = True
        baseinfo = False
        shareholder = False
        employe = False
        branch = False
        nerecitempub = False
        liquidation = False
        changerecord = False
        mortreginfo = False
        equitypledge = False
        trademark = False
        checkinformation = False
        drraninsres = False
        administrativelicensing = False
        administrativepenalty = False
        exceptioninformation = False
        judicialinformation = False
        illegalinformation = False
        self_administrativelicensing = False
        self_shareholder = False
        self_insalterstockinfo = False
        self_propledgereginfo = False
        self_appsimplecancelobjection = False
        self_epubgroupmenberinfo = False
        self_abolishmentlicenseinfo = False
        
        
        [KAFKA]
        KAFKA_TOPIC: the kafka topic which save resp.text when download detail
        KAFKA_BOOTSTRAP_SERVERS: the kafka server which save resp.text when download detail
        KAFKA_GROUP = the group name of consumer
        
        [REDIS_TAG]
        REDIS_TAG_HOST: redis server
        REDIS_TAG_PORT: redis port 
        REDIS_TAG_DB: redis db
        REDIS_TAG_EX: the expiration time of tag which judge use abuyun or normal http proxy
        
        
        [MONGO]
        MONGODB_SERVER: the mongo server which save detail resp.text
        MONGODB_PORT: the mongo port
        MONGODB_DB: th emongo db name which store detail resp.text
        MONGODB_COLLECTION: the mongo collection which sore detail resp.text
        MONGODB_USER: the user name of mongodb
        MONGODB_PASSWORD: the password of mongo db
        MONGODB_COLLECTIONS = [{"host": mysql_pipline get data to write in mysql from this mongo server,
                                "port": the mongo port,
                                "db": name of mongo db,
                                "collection": name of mongo collection,
                                "user": name of mongo user,
                                "password": password of mongodb
                              }
                              ]
        
        
        [MYSQL_PIPLINE]
        TIMES: the time of parser run one round
        EVERYTIME:  the number of record parse a time 
        DETAIL_EVERYTIME: the num of companies write in mysql one time
        PROCESSING_NUMBER: the number of process which run pipline
        QUEUE_NUMBER: whe length of queue which store parse result
        
        
        [UPDATE_GENERATOR]
        QUEUE_SIZE: the length of queue in generator which store creditcode
        ADD_TO_QUEUE_EVERYTIME: the num of creditcodes push into queue every time
        ANNUAL_SRART_ID_TAG: the tag mark the company which is the last company to update last time
        ANNUAL_ROUND_START_TYPE_TAG: mark the type of this update round
        
        
        cd tools
        python GSXT_MOBILE_GENERATOR_SCRIPT.py UPDATE_ANNUAL GSXT_MOBILE_ANNUAL_config.ini
        python GSXT_MOBILE_DISCOVER_SCRIPT.py ANNUAL GSXT_MOBILE_ANNUAL_config.ini
        python GSXT_MOBILE_PIPLINE_SCRIPT.py DETAIL GSXT_MOBILE_ANNUAL_config.ini
        python GSXT_MOBILE_PIPLINE_SCRIPT.py DETAIL_MYSQL GSXT_MOBILE_ANNUAL_config.ini


Mode4: update or replenish annual and write them into MySQL  Three Second
         modify  GSXT_MOBILE_ANNUAL_config.ini
         
         [API]
        DETAIL_COMPANY_URL: from this url get creditcode to download detail
        PROXY_API: from this url get normal http proxy
        
        
        [LOG]
        DIR: the log dir for downloader
        MYSQL_PIPLINE_LOG_DIR: the log dir for mysql_pipline
        
        
        [COHERSOUP]
        DIR: the dir for COHERSOUP/cohersoup
        
        
        [DOWNLOADER]
         NUMBER = 0  count the num of companies downoader downloading, keep the default
         THRESHOLD: the most num of companies can the downloader download at the same time
         THRESHOLD_429: if the percentage of 429(resp_coed) bigger than this rate, won't use abuyun and change to use 
                       normal http proxy
         THRESHOLD_403: if the percentage of 403(resp_coed) bigger than this rate, won't use abuyun and change to use 
                       normal http proxy
         SEM_NUM: the max number of requests when use asyncio
         UA: default useragent 
        
        [ANNUAL_NEED] : the downloader decide which message to download from this tag when download normal detail
        annual = True
        baseinfo = False
        shareholder = False
        employe = False
        branch = False
        nerecitempub = False
        liquidation = False
        changerecord = False
        mortreginfo = False
        equitypledge = False
        trademark = False
        checkinformation = False
        drraninsres = False
        administrativelicensing = False
        administrativepenalty = False
        exceptioninformation = False
        judicialinformation = False
        illegalinformation = False
        self_administrativelicensing = False
        self_shareholder = False
        self_insalterstockinfo = False
        self_propledgereginfo = False
        self_appsimplecancelobjection = False
        self_epubgroupmenberinfo = False
        self_abolishmentlicenseinfo = False
        
        
        [KAFKA]
        KAFKA_TOPIC: the kafka topic which save resp.text when download detail
        KAFKA_BOOTSTRAP_SERVERS: the kafka server which save resp.text when download detail
        KAFKA_GROUP = the group name of consumer
        
        [REDIS_TAG]
        REDIS_TAG_HOST: redis server
        REDIS_TAG_PORT: redis port 
        REDIS_TAG_DB: redis db
        REDIS_TAG_EX: the expiration time of tag which judge use abuyun or normal http proxy
        
        
        [MONGO]
        MONGODB_SERVER: the mongo server which save detail resp.text
        MONGODB_PORT: the mongo port
        MONGODB_DB: th emongo db name which store detail resp.text
        MONGODB_COLLECTION: the mongo collection which sore detail resp.text
        MONGODB_USER: the user name of mongodb
        MONGODB_PASSWORD: the password of mongo db
        MONGODB_COLLECTIONS = [{"host": mysql_pipline get data to write in mysql from this mongo server,
                                "port": the mongo port,
                                "db": name of mongo db,
                                "collection": name of mongo collection,
                                "user": name of mongo user,
                                "password": password of mongodb
                              }
                              ]
        
        
        [MYSQL_PIPLINE]
        TIMES: the time of parser run one round
        EVERYTIME:  the number of record parse a time 
        DETAIL_EVERYTIME: the num of companies write in mysql one time
        PROCESSING_NUMBER: the number of process which run pipline
        QUEUE_NUMBER: whe length of queue which store parse result
        
        
        [UPDATE_GENERATOR]
        QUEUE_SIZE: the length of queue in generator which store creditcode
        ADD_TO_QUEUE_EVERYTIME: the num of creditcodes push into queue every time
        ANNUAL_SRART_ID_TAG: the tag mark the company which is the last company to update last time
        ANNUAL_ROUND_START_TYPE_TAG: mark the type of this update round
        
        
        cd tools
        python GSXT_MOBILE_GENERATOR_SCRIPT.py THREE_SECOND GSXT_MOBILE_ANNUAL_THREE_SECOND_config.ini
        python GSXT_MOBILE_DISCOVER_SCRIPT.py ANNUAL GSXT_MOBILE_ANNUAL_THREE_SECOND_config.ini
        python GSXT_MOBILE_PIPLINE_SCRIPT.py DETAIL GSXT_MOBILE_ANNUAL_config.ini
        python GSXT_MOBILE_PIPLINE_SCRIPT.py DETAIL_MYSQL GSXT_MOBILE_ANNUAL_config.ini
```


