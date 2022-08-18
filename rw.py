#!/usr/bin/env python

import os
import sys

java_path = 'java'
mainClass = 'org.kr.db.migrator.rw.RWMain'
javaArgs = list()
javaArgs.append('-server')
javaArgs.append('-Dfile.encoding=UTF-8')
javaArgs.append('-d64')
javaArgs.append('-Xmx4g')
javaArgs.append('-Xms4g')
javaArgs.append('-Xmn128m')
javaArgs.append('-Xss1m')
javaArgs.append('-XX:+UseConcMarkSweepGC')
javaArgs.append('-XX:+DoEscapeAnalysis')
javaArgs.append('-XX:PermSize=256m')
javaArgs.append('-XX:MaxPermSize=256m')
javaArgs.append('-XX:+AggressiveOpts')
if sys.platform.startswith('win'):
    javaArgs.append('-cp "lib/*;rw.jar"')
else:
    javaArgs.append('-cp "lib/*:rw.jar"')

if len(sys.argv) > 1
    javaArgs.append(sys.argv[1])

command = java_path + ' '
for arg in javaArgs:
    command += arg + ' '
command += mainClass

os.system(command)
sys.exit()
