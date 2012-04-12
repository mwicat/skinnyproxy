import sys; sys.path.append('.')
import proxy

db_worker = proxy.DBWorker()
db_worker.start()

def say(message='hi'): print message

db_worker.launch(say)
db_worker.launch(say, 'goodbye')
