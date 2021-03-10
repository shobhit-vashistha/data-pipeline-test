PWD=`pwd`

# stop and remove previous container
sudo docker stop sb-dp-test && sudo docker rm sb-dp-test
# build the image
sudo docker build -t sb-dp-test-image:1.1 src/.
# run container
sudo docker run -d --name=sb-dp-test sb-dp-test-image:1.1
# open logs
sudo docker logs -f sb-dp-test
