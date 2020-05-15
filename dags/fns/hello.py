def collector(ds, **context):
  message = "hello"
  return message

def printer(ds, **context):
  data = context['task_instance'].xcom_pull(task_ids='collector')
  print(data)
