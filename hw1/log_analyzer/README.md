# Log Analyzer
## Config:
The configuration is a json file with following fields:
* *MAX_REPORT_SIZE* - Max report size. **Required.** <br>
* *REPORTS_DIR* - Path to directory with reports. **Required.** <br>
* *LOGS_DIR* - Path to directory witn server logs. **Required.** <br>
* *MONITORING_LOG_FILE* - Program log file path. *Optional.* <br>
* *TIMESTAMP_FILE* - Path to timestamp file. *Optional.* <br>

Default config:
```json
{  
    "MAX_REPORT_SIZE": 1000,
    "REPORTS_DIR": "./reports",
    "LOGS_DIR": "./log",
}
```

## Usage:
```
log_analyzer.py [-h] [--config CONFIG_PATH]

optional arguments:
  -h, --help           show this help message and exit
  --config CONFIG      config file path
```

## Tests usage: 
```
python -m unittest discover -s ./log_analyzer
```
