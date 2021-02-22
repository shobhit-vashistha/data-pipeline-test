sudo docker build -t sb-dp-test-image:1.0 .

sudo docker run -d --name=sb-dp-test sb-dp-test-image:1.0

sudo docker stop b-dp-test

sudo docker logs -f sb-dp-test
