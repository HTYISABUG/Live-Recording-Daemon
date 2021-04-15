import json
import os
import subprocess
import glob
import time

with open('settings.json') as fp:
    settings = json.load(fp)
    savepath = settings['savepath']

prev = []

try:
    while True:
        p = subprocess.run(
            ['du', '-h', '-d0', *glob.glob(os.path.join(savepath, '*'))],
            stdout=subprocess.PIPE,
        )

        output = p.stdout.decode().strip().split('\n')

        diff = [line for line in output if line not in prev]

        print('\033c')
        print('\n'.join(diff))

        prev = output

        time.sleep(1)
except KeyboardInterrupt:
    pass
