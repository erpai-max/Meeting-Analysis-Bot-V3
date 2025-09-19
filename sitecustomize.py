# sitecustomize.py
import os
os.environ.setdefault("GRPC_VERBOSITY", "NONE")
os.environ.setdefault("GRPC_CPP_VERBOSITY", "NONE")
# optional extras to quiet other native libs:
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")      # TensorFlow/absl noise
os.environ.setdefault("ABSL_LOGGING_MIN_LOG_LEVEL", "3") # absl python logs
