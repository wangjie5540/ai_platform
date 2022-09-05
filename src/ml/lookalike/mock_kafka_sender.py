# coding: utf-8

from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['bigdata-server-10:9092'])
msg = {
    "seedCrowdFileUrl": "http://172.21.32.6:8658/api/labelx/open/pack/204/download?targetEntityId=2",
    "downloadToken": "eyJkZXBsb3lNb2RlIjoiU0hBUkUiLCJuYW1lc3BhY2UiOiJERUZBVUxUIiwib3JnQ29kZSI6ImRpZ2l0Zm9yY2UiLCJvcmdDb2RlUGF0aCI6ImRpZ2l0Zm9yY2UiLCJvcmdOYW1lIjoi5pWw5Yq/5LqR5Yib56eR5oqA6IKh5Lu95pyJ6ZmQ5YWs5Y+4Iiwib3JnTmFtZVBhdGgiOiLmlbDlir/kupHliJvnp5HmioDogqHku73mnInpmZDlhazlj7giLCJ0ZW5hbnRJZCI6MTAwMDAsInVzZXJBY2NvdW50Ijoic3VwZXJhZG1pbiIsInVzZXJOYW1lIjoi6LaF57qn566h55CG5ZGYIn0=",
    "scene": "1",
    "businessId": "641efe837a8d4850bc42a8d906385a20",
    "whereSql": "1=1",
    "solutionId": "2",
    "targetLimit": 100,
}
msg = json.dumps(msg)
producer.send(topic="cd_pack_algorithm_lookalike", value=msg.encode())
producer.flush()