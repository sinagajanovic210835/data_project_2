#! /bin/bash

DIR=`cat /bool/cnt.txt`
FILEINV=
FILEPRO=
FILECNT=
MESSAGE=
PERIOD=$1
TEST=$((PERIOD * 60))

# hdfs dfs -mkdir /user/test/invoices
# sleep 5
# hdfs dfs -mkdir /user/test/invoices/1
# sleep 5

for YEAR in 2010 2011; do
    for MONTH in 12 1 2 3 4 5 6 7 8 9 10 11; do
        
        for DAY in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31; do  
            hdfs dfs -rm -r /user/test/products/
            hdfs dfs -rm -r /user/test/countries/
            NAME=$DAY'_'$MONTH'_'$YEAR'.csv'            
            FILEPRO=`ls /data1/products | grep -w $NAME`
            FILECNT=`ls /data1/countries/ | grep -w $NAME`
            if [[ $FILEPRO != "" ]]; then               
                hdfs dfs -mkdir /user/test/products/
                hdfs dfs -appendToFile /data1/products/$FILEPRO /user/test/products/products.csv
                rm /data1/products/$FILEPRO
                MESSAGE+=product  
                echo $MESSAGE              
            fi            
            if [[ $FILECNT != "" ]]; then               
                hdfs dfs -mkdir /user/test/countries/
                hdfs dfs -appendToFile /data1/countries/$FILECNT /user/test/countries/countries.csv
                rm /data1/countries/$FILECNT
                MESSAGE+=country
                echo $MESSAGE               
            fi                  
            sleep 2            
            for HOUR in {0..23}; do
            ##############################################################################                                          
                END=$((HOUR + 1))
                INVOICE=$HOUR'_'$END'_'$DAY'_'$MONTH'_'$YEAR'.csv'
                FILEINV=`ls /data1/invoices/ | grep -w $INVOICE`
                if [[ $FILEINV != "" ]]; then               
                    hdfs dfs -appendToFile /data1/invoices/$FILEINV /user/test/invoices/$DIR/invoice.csv
                    rm /data1/invoices/$FILEINV
                fi
                if [ `expr $END % 4` -eq 0 ]; then                   
                    OLDDIR=$((DIR - 1))
                    MESSAGE+=invoice_$DIR
                    DIR=$((DIR + 1))
                    hdfs dfs -mkdir /user/test/invoices/$DIR
                    hdfs dfs -rm -r /user/test/invoices/$OLDDIR                                                      
                fi                                                                               
                MOD=5
                while [[ $MOD != 0 ]]
                do
                    SEC=`date +%s`
                    MOD=$((SEC % TEST))
                    sleep 1                    
                done
                echo '.......'$MESSAGE'................'$INVOICE  > /bool/inf.txt
                echo $MESSAGE > /bool/bool.txt    
                echo $DIR > /bool/cnt.txt                      
                MESSAGE=                
            ##################################################################################          
            done
        done
    done
done
