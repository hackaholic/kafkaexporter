import prometheus_client
from jmxquery import *
import time

blacklist = ["__consumer_offsets"]
kafka_counters = {}

class JMXExporter:
  def __init__(self, jmx_port=9999):
    self.jmxUrl = f"service:jmx:rmi:///jndi/rmi://localhost:{jmx_port}/jmxrmi"
    
  def get_jmx_connection(self):
    self.jmxConnection = JMXConnection(self.jmxUrl)

  def execute_query(self, query="*:*"):
    try:
      jmxquery = [JMXQuery(f"{query}")]
      metrics = self.jmxConnection.query(jmxquery)
      return metrics
    except Exception as e:
      print(e)

def parsemetrics(data):
  mBeanDict =  dict(map(lambda x: x.split("="), f"{data.mBeanName}".split(",")))
  if mBeanDict.get("topic") not in blacklist and data.value_type.lower() in ['integer', 'double', 'long']:
  #  if f"{data.mBeanName}".startswith("kafka.log:type"):
  #    print(mBeanDict, data.value)
  #    tag_dict = {key:value for key, value in mBeanDict.items() if key not in ["kafka.log:type", "name"]}
  #    counter_name = mBeanDict.get('name', '').lower().replace("-", "_")
  #    if not kafka_counters.get(f"kafka_topic_{counter_name}"):
  #       counter = prometheus_client.Gauge(f"kafka_topic_{counter_name}", f"kafka topic {counter_name}", tag_dict.keys())
  #       kafka_counters[f"kafka_topic_{counter_name}"] = counter
  #    kafka_counters[f"kafka_topic_{counter_name}"].labels(**tag_dict).set(data.value)
    #print(mBeanDict, data.value, data.attribute, data.value_type)
    #tag_dict = {key:value for key, value in mBeanDict.items() if key != "name" and not key.endswith(":type")}
    #counter_name = f"{''.join(key.split(':')[0].replace('.', '_') for key in mBeanDict if key.endswith(':type'))}"
    counter_name = ""
    tag_dict = {}
    for key, value in mBeanDict.items():
      key = key.replace("-", "_")
      if key != "name" and not key.endswith(":type"):
        tag_dict[key] = value
      if key.endswith(":type"):
        counter_name = f"{key.split(':')[0]}_{value.lower()}"
    if "name" in mBeanDict:
      counter_name += f"_{mBeanDict.get('name').lower()}_{data.attribute.lower()}"
    else:
      counter_name += f"_{data.attribute.lower()}"
    counter_name = counter_name.replace(".", "_").replace("-", "_")
    #print(counter_name)
    if not kafka_counters.get(counter_name):
      counter = prometheus_client.Gauge(counter_name, counter_name, tag_dict.keys())
      kafka_counters[counter_name] = counter
    kafka_counters[counter_name].labels(**tag_dict).set(data.value)
       
     
def main():
  print("Starting kafka exporter...")
  prometheus_client.start_http_server(7072)
  while True:
    jmx_scrape_interval = os.getenv("JMX_SCRAPE_INTERVAL", 10)
    time.sleep(jmx_scrape_interval)
    obj = JMXExporter()
    obj.get_jmx_connection()
    metrics = obj.execute_query()
    for data in metrics:
      try:
        print(data.to_string())
        print(data.value, data.value_type, data.attribute)
        #print(dict(map(lambda x: x.split("="), f"{data.mBeanName}".split(","))), data.value)
        parsemetrics(data)
      except Exception as e:
        print(e)
    
if __name__ == "__main__":
  main()
