# Set up a grip runtime for testing/development
FROM catthehacker/ubuntu:full-latest

RUN wget https://go.dev/dl/go1.19.linux-amd64.tar.gz
RUN rm -rf /usr/local/go && tar -C /usr/local -xzf go1.19.linux-amd64.tar.gz

RUN pip install requests numpy PyYAML

ENV GOPATH=/usr/local/go
ENV PATH="/usr/local/go/bin:${PATH}"

#  Keep the Container Running
ENTRYPOINT ["tail", "-f", "/dev/null"]

# build
# `--platform` for MAC silicon 
# docker build  --platform linux/arm64/v8  -t grip-test .

# launch
# docker run --name grip-test -v $(pwd):/src -d -t grip-test 

# test
# docker exec -it grip-test bash