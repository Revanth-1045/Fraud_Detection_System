import sys
import os
print("Python Executable:", sys.executable)
print("Python Version:", sys.version)
print("Sys Path:", sys.path)
try:
    import boto3
    print("Boto3 imported successfully from", boto3.__file__)
except ImportError as e:
    print("ImportError:", e)
