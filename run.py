import sys
import os


python_path = sys.executable
env_path = os.path.dirname(os.path.dirname(python_path))
env_name = os.path.basename(env_path)
if env_name != 'ENV':
    raise Exception('Failed to get model directory')

root_path = os.path.dirname(env_path)
model_path = os.path.join(root_path, 'MODEL')
if not os.path.exists(model_path):
    os.mkdir(model_path)
print model_path
