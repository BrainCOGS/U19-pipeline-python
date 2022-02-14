

cd $1

module load matlab/R2020b

key = $2 
matlab -singleCompThread -nodisplay -nosplash -r populate_ScanInfo_spock(key);