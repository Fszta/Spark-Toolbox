IMAGES=(spark-base spark-master spark-worker)
TAG=3.0.1

buildImage() {
  IMAGE_NAME=$1:${TAG}
  cd $1
  echo "Start building ${IMAGE_NAME}"
  docker build -t ${IMAGE_NAME} .
  cd ..
}

for image in ${IMAGES[@]}; do
  buildImage ${image}
done


