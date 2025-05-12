#!/bin/sh
set -e
ACCEPT=$1
SDK_VERSION="2.0-L1"
pythoncheck() {
if which python3 > /dev/null 2>&1; then
		major_ver=$(python3 -c"import sys; print(sys.version_info.major)")
		minor_ver=$(python3 -c"import sys; print(sys.version_info.minor)")
		if [ $major_ver -eq 3 ] && [ $minor_ver -eq 8 ]; then
			echo "$(tput setaf 2)Supported Python exists.$(tput sgr 0)"
		else 
			echo "$(tput setaf 1)Unsupported python version.$(tput sgr 0)"
			exit
		fi
	else
		echo "$(tput setaf 1)Python environment doesnt exists.$(tput sgr 0)"
	fi
}

agreement() {
echo "$(tput setaf 3)By installing this software, you agree to the End User License Agreement for Research Use.$(tput sgr 0)"
echo "$(tput setaf 3)Type 'y' to accept.$(tput sgr 0)"
read -p "Enter your response:"  ACCEPT
}

installSdk() {
apt-get install -y gdebi
echo "$(tput setaf 2)Installing PathologySDK"$SDK_VERSION" modules please wait... $(tput sgr 0)"
gdebi -n ./pathologysdk-modules/*pixelengine*.deb
gdebi -n ./pathologysdk-python38-modules/*python3-pixelengine*.deb
gdebi -n ./pathologysdk-modules/*eglrendercontext*.deb
gdebi -n ./pathologysdk-python38-modules/*python3-eglrendercontext*.deb
gdebi -n ./pathologysdk-modules/*gles2renderbackend*.deb
gdebi -n ./pathologysdk-python38-modules/*python3-gles2renderbackend*.deb
gdebi -n ./pathologysdk-modules/*gles3renderbackend*.deb
gdebi -n ./pathologysdk-python38-modules/*python3-gles3renderbackend*.deb
gdebi -n ./pathologysdk-modules/*softwarerenderer*.deb
gdebi -n ./pathologysdk-python38-modules/*python3-softwarerenderbackend*.deb
gdebi -n ./pathologysdk-python38-modules/*python3-softwarerendercontext*.deb
echo "$(tput setaf 2)PathologySDK"$SDK_VERSION" successfully installed$(tput sgr 0)"
}

installSdk
