dest=$2
mkdir -p $dest;


while read p; do

      echo "$p"
      wget -P $dest -c --user carolinux --password Idontremember1 $p
  done <$1
