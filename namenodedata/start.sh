#! /bin/bash

FILEINV=
FILEPRO=
FILECNT=
MESSAGE=
PERIOD=$1
TEST=$((PERIOD * 60))
source /bool/vars.txt
MAX=32

function checkmonth() {
    for MNT in 1 3 5 7 8 10 12; do
        if [ `expr $1` -eq $MNT ]; then
            MAX=32 
        fi
    done
    for MNT in 4 6 9 11; do
        if [ `expr $1` -eq $MNT ]; then
            MAX=31
        fi    
    done        
    if [ `expr $1` -eq 2 ]; then		
        MAX=29
    fi   
}

if [ `expr $HOUR` -eq 25 ]; then
    HOUR=1
fi

if [ `expr $DIR` -eq 1 ]; then
    hdfs dfs -mkdir /user/test/invoices
    sleep 5
    hdfs dfs -mkdir /user/test/invoices/1
fi

sleep 5

######################################################################################################

while [ `expr $YEAR` -lt 2012 ]; do

    while [ `expr $MONTH` -lt 13 ]; do

    	checkmonth $MONTH  

    	while [ `expr $DAY` -lt $MAX ]; do
        
            if [ `expr $HOUR` -eq 1 ]; then
                hdfs dfs -rm -r /user/test/products/
                hdfs dfs -rm -r /user/test/countries/
                NAME=$DAY'_'$MONTH'_'$YEAR'.csv'            
                FILEPRO=`ls /data1/products | grep -w $NAME`
                FILECNT=`ls /data1/countries/ | grep -w $NAME`

                if [[ $FILEPRO != "" ]]; then               
                    hdfs dfs -mkdir /user/test/products/
                    hdfs dfs -appendToFile /data1/products/$FILEPRO /user/test/products/products.csv
                    # rm /data1/products/$FILEPRO
                    MESSAGE+=product                            
                fi    

                if [[ $FILECNT != "" ]]; then               
                    hdfs dfs -mkdir /user/test/countries/
                    hdfs dfs -appendToFile /data1/countries/$FILECNT /user/test/countries/countries.csv
                    # rm /data1/countries/$FILECNT
                    MESSAGE+=country                           
                fi 
            fi

            while [ `expr $HOUR` -lt 25 ]; do
                MOD=5
                while [[ $MOD != 0 ]]; do
                    SEC=`date +%s`
                    MOD=$((SEC % TEST))
                    sleep 1                    
                done
                ######################################################################################                                          
                START=$((HOUR - 1))
                INVOICE=$START'_'$HOUR'_'$DAY'_'$MONTH'_'$YEAR'.csv'
                FILEINV=`ls /data1/invoices/ | grep -w $INVOICE`

                if [[ $FILEINV != "" ]]; then               
                    hdfs dfs -appendToFile /data1/invoices/$FILEINV /user/test/invoices/$DIR/invoice.csv
                    # rm /data1/invoices/$FILEINV
                fi

                if [ `expr $HOUR % 4` -eq 0 ]; then                   
                    OLDDIR=$((DIR - 1))
                    MESSAGE+=invoice_$DIR
                    DIR=$((DIR + 1))
                    hdfs dfs -mkdir /user/test/invoices/$DIR
                    hdfs dfs -rm -r /user/test/invoices/$OLDDIR                                                      
                fi  

                echo '.......'$MESSAGE'................'$INVOICE  > /bool/inf.txt
                echo $MESSAGE > /bool/bool.txt                        
                MESSAGE=   
                HOUR=$((HOUR + 1))                 
                echo "HOUR=$HOUR" > /bool/vars.txt   
                echo "MONTH=$MONTH" >> /bool/vars.txt
                echo "YEAR=$YEAR" >> /bool/vars.txt
                echo "DAY=$DAY" >> /bool/vars.txt 
                echo "DIR=$DIR" >> /bool/vars.txt                 
                ######################################################################################             
            done
            HOUR=1
            DAY=$((DAY + 1))	
    	done
    	DAY=1
    	MONTH=$((MONTH + 1))
    done
    MONTH=1		
    YEAR=$((YEAR + 1))
done
######################################################################################################
