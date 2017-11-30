# Log Analyzer
## Config:
The configuration is a json file with following fields:
* *REPORT_SIZE* - Max report size. **Required.** <br>
* *REPORT_DIR* - Path to directory with reports. **Required.** <br>
* *LOG_DIR* - Path to directory witn server logs. **Required.** <br>
* *OUTPUT_LOG_FILE* - Program log file path. *Optional.* <br>
* *TIMESTAMP_FILE* - Path to timestamp file. *Optional.* <br>

Default config:
```json
{  
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
}
```

## Usage:
```
log_analyzer.py [-h] [--config CONFIG_PATH]

optional arguments:
  -h, --help           show this help message and exit
  --config CONFIG      config file path
```

## Tests: 
Not yet
