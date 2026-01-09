FROM public.ecr.aws/glue/aws-glue-libs:5

USER root

COPY infra/requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

ENV AWS_ACCESS_KEY_ID=minioadmin
ENV AWS_SECRET_ACCESS_KEY=minioadmin123
ENV AWS_ENDPOINT_URL=http://minio:9000
ENV DISABLE_SSL=true

WORKDIR /home/hadoop/workspace

USER hadoop