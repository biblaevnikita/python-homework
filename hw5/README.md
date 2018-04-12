Асинхронный HTTP-сервер на основе [asyncore-epoll](https://github.com/m13253/python-asyncore-epoll).

## Пример запуска
```
python httpd.py -a 127.0.0.1 -p 8080 -w 4 -d /some/folder
```

## Параметры запуска
```
-a - адрес сервера
-p - порт
-w - количество воркеров
-r - путь до каталога с контентом
```

## Нагрузочное тестирование
### Окружение:

1,6 GHz Intel Core i5, 8 ГБ 1600 MHz DDR3

CentOS Linux release 7.4.1708 (Core)

### ApacheBench
`ab ‑n 50000 ‑c 100 ‑r http://127.0.0.1:8080/`
```
Server Software:        DunnoServer
Server Hostname:        127.0.0.1
Server Port:            8080

Document Path:          /
Document Length:        12 bytes

Concurrency Level:      100
Time taken for tests:   63.489 seconds
Complete requests:      50000
Failed requests:        0
Write errors:           0
Total transferred:      7650000 bytes
HTML transferred:       600000 bytes
Requests per second:    787.53 [#/sec] (mean)
Time per request:       126.979 [ms] (mean)
Time per request:       1.270 [ms] (mean, across all concurrent requests)
Transfer rate:          117.67 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0   89 384.6      0   15379
Processing:     1   34 111.4     20    6633
Waiting:        0   33 111.4     19    6633
Total:          1  123 429.8     21   15386

Percentage of the requests served within a certain time (ms)
  50%     21
  66%     29
  75%     34
  80%     38
  90%     53
  95%   1053
  98%   1285
  99%   1508
 100%  15386 (longest request)
 ```
 
 ### wrk
 `wrk -t12 -c100 -d60 http://127.0.0.1:8080/`
 ```
 Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    38.72ms  111.52ms   1.79s    94.30%
    Req/Sec    79.16     54.70   420.00     70.33%
  49131 requests in 1.00m, 7.17MB read
  Socket errors: connect 0, read 0, write 0, timeout 7
Requests/sec:    818.21
Transfer/sec:    122.25KB
```
