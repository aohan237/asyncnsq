# NSQ Test Service

Start a local 3-node NSQ test cluster:

```bash
./test_service/start.sh
```

Stop and remove containers plus test volumes:

```bash
./test_service/stop.sh
```

Ports:

- `nsqlookupd`: `4160` TCP, `4161` HTTP
- `nsqd1`: `4150` TCP, `4151` HTTP
- `nsqd2`: `4250` TCP, `4251` HTTP
- `nsqd3`: `4350` TCP, `4351` HTTP
- `nsqadmin`: `4171` HTTP

Run the full integration suite while the cluster is up:

```bash
uv run python -m pytest --cov=asyncnsq --cov-report=term-missing
```
