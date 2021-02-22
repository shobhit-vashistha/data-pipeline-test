sudo docker build -t sb-dp-test-image:1.0 .
sudo docker run -i --log-driver=none -a stdin -a stdout -a stderr sb-dp-test-image:1.0
sudo docker container ls
sudo docker stop <id>
