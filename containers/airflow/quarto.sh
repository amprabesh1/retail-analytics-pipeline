#!/bin/bash

# Pick the Quarto build for this CPU (x86_64 -> amd64, aarch64 -> arm64).
case "$(uname -m)" in
  aarch64|arm64) QARCH=arm64 ;;
  *) QARCH=amd64 ;;
esac

curl -L -o ~/quarto-1.5.43-linux-${QARCH}.tar.gz https://github.com/quarto-dev/quarto-cli/releases/download/v1.5.43/quarto-1.5.43-linux-${QARCH}.tar.gz
mkdir ~/opt
tar -C ~/opt -xvzf ~/quarto-1.5.43-linux-${QARCH}.tar.gz

mkdir ~/.local/bin
ln -s ~/opt/quarto-1.5.43/bin/quarto ~/.local/bin/quarto

( echo ""; echo 'export PATH=$PATH:~/.local/bin\n' ; echo "" ) >> ~/.profile
source ~/.profile
