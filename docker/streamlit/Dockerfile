FROM python:3.12-slim-bookworm
WORKDIR /workspace
COPY dashboard/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt 'boto3>=1.35,<2' 'redis>=5,<7' 'prometheus-client>=0.21,<1'
COPY dashboard /workspace/dashboard
COPY streamlit_app.py /workspace/streamlit_app.py
COPY .streamlit /workspace/.streamlit
COPY observatory /workspace/observatory
CMD ["python", "-m", "streamlit", "run", "streamlit_app.py", "--server.address=0.0.0.0", "--server.port=8501"]
