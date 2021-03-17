PWD=`pwd`

# stop and remove previous container
sudo docker stop sb-dp-test-server && sudo docker rm sb-dp-test-server
# build the image
sudo docker build -t sb-dp-test-server-image:1.1 .
# run container
sudo docker run -p 6660:6660 -d --name=sb-dp-test-server sb-dp-test-server-image:1.1
# open logs
sudo docker logs -f sb-dp-test-server
