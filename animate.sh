#!/bin/sh

for name in $@
do
  x=1
  dir=target/export/$name/analysis
  if [ -d $dir ] ; then
    (
      cd $dir
      rm -Rf animation
      mkdir animation
      cd animation
      for f in `ls ../*.png`
      do
        counter=$(printf %03d $x)
        ln -s "$f" img"$counter".png
        x=$(($x+1))
      done
      ls
      ffmpeg -f image2 -qscale 5 -r 15 -b 9600 -i img%03d.png ../../../${name}_movie.mp4
    )
  else
    echo "No such directory $dir"
  fi
done
