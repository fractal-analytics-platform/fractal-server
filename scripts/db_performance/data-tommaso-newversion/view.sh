gnuplot -e "set datafile separator ','; set g; p 'out.csv' u 2:(\$1==5?\$4:1/0) w lp pt 7 ps 2" --persist
