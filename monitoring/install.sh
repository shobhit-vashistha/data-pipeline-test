PWD=`pwd`

# stop and remove previous container
sudo docker stop sb-dp-test-server && sudo docker rm sb-dp-test-server
# build the image
sudo docker build -t sb-dp-test-server-image:1.1 .
# run container
sudo docker run -p 6666:6666 -d --name=sb-dp-test sb-dp-test-server-image:1.1
# open logs
sudo docker logs -f sb-dp-test-server
