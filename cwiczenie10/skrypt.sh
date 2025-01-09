#!/usr/bin/env bash

#######################################################################
# Bazy danych przestrzennych 2 - Ćwiczenie 10
# Michał Sawicki - Indeks 407554
# Skrypt automatyzujący przetwarzanie danych i integrację z bazą MySQL.
# 
# Opis:
# 1. Pobiera plik ZIP z Internetu i rozpakowuje jego zawartość.
# 2. Waliduje dane, odrzuca błędne wiersze i przetwarza dane wejściowe.
# 3. Tworzy tabelę w bazie MySQL i ładuje do niej dane.
# 4. Eksportuje dane do pliku CSV, kompresuje plik i zapisuje logi działań.
#######################################################################

########################
# Konfiguracja
########################

STUDENT_INDEX="407554"
TIMESTAMP=$(date +%m%d%Y)
LOG_DIR="PROCESSED"
LOG_FILE="${LOG_DIR}/script_${TIMESTAMP}.log"

URL="http://home.agh.edu.pl/~wsarlej/dyd/bdp2/materialy/cw10/InternetSales_new.zip"
ZIP_PASS_B64="YmRwMmFnaAo="
ZIP_PASS=$(echo "$ZIP_PASS_B64" | base64 --decode)

SQL_HOST="127.0.0.1"
SQL_USER="root"
SQL_PASS_B64="Y3dpY3plbmllMTA="
SQL_PASS=$(echo "$SQL_PASS_B64" | base64 --decode)

DB_NAME="cwiczenie10"
TABLE_NAME="CUSTOMERS_${STUDENT_INDEX}"

TMP_DIR="tmp"
DOWNLOAD_FILE="${TMP_DIR}/InternetSales_new.zip"
RAW_FILE="InternetSales_new.txt"
VALIDATED_FILE="${TMP_DIR}/InternetSales_new_validated.txt"
BAD_FILE="${LOG_DIR}/InternetSales_new.bad_${TIMESTAMP}.txt"
PROCESSED_CSV="${LOG_DIR}/${TIMESTAMP}_InternetSales_processed.csv"
PROCESSED_ZIP="${PROCESSED_CSV}.zip"

mkdir -p "$LOG_DIR"
mkdir -p "$TMP_DIR"

########################
# Funkcje pomocnicze
########################

log() {
    local MESSAGE="$1"
    local DATESTR=$(date +%Y%m%d%H%M%S)

    echo "${DATESTR} - ${MESSAGE}" >> "$LOG_FILE"
    echo "${DATESTR} - ${MESSAGE}"
}

check_success() {
    local RESULT="$1"
    local EXIT_CODE="$2"

    if [ $EXIT_CODE -eq 0 ]; then
        log "${RESULT} - Successful"
    else
        log "${RESULT} - Failed"
        exit 1
    fi
}

######################################
# 1. Pobieranie pliku z Internetu
######################################

log "Downloading file"

wget -q "$URL" -O "$DOWNLOAD_FILE"
check_success "Download step" $?

######################################
# 2. Rozpakowywanie pliku ZIP
######################################

log "Unzipping file"

unzip -P "$ZIP_PASS" "$DOWNLOAD_FILE" -d "$TMP_DIR" > /dev/null
check_success "Unzipping step" $?

mv "$TMP_DIR/InternetSales_new.txt" "$RAW_FILE"

######################################
# 3. Walidacja pliku
######################################

log "Validating file"

HEADER=$(head -n 1 "$RAW_FILE")
HEADER_COL_COUNT=$(echo "$HEADER" | awk -F'|' '{print NF}')

echo "$HEADER" > "$VALIDATED_FILE"
echo "$HEADER" > "$BAD_FILE"

awk -F'|' -v hcc="$HEADER_COL_COUNT" -v bad_file="$BAD_FILE" -v validated_file="$VALIDATED_FILE" -v ofs="|" '
BEGIN { OFS = ofs }
NR == 1 { next }
{
    if (NF != hcc || $5 > 100 || $7 != "") {
        print $0 >> bad_file
    } else {
        split($3, names, ",");
        if (length(names[1]) > 0 && length(names[2]) > 0) {
            print $1, $2, names[2], names[1], $4, $5, $6, $7 >> validated_file
        } else {
            print $0 >> bad_file
        }
    }
}' "$RAW_FILE"
check_success "Validation step" $?

######################################
# 4. Tworzenie tabeli MySQL
######################################

log "Creating MySQL table $TABLE_NAME"

mysql -h "$SQL_HOST" -u "$SQL_USER" -p"$SQL_PASS" "$DB_NAME" <<EOF
DROP TABLE IF EXISTS $TABLE_NAME;
CREATE TABLE $TABLE_NAME (
    ProductKey INT,
    CurrencyAlternateKey VARCHAR(50),
    FIRST_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    OrderDateKey DATE,
    OrderQuantity INT,
    UnitPrice DECIMAL(10,2),
    SecretCode VARCHAR(50)
);
EOF
check_success "Table creation step" $?

######################################
# 5. Ładowanie danych do tabeli MySQL
######################################

log "Loading data into MySQL table"

mysql --local-infile=1 -h "$SQL_HOST" -u "$SQL_USER" -p"$SQL_PASS" "$DB_NAME" <<EOF
LOAD DATA LOCAL INFILE '$(pwd)/$VALIDATED_FILE'
INTO TABLE $TABLE_NAME
FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' IGNORE 1 LINES
(ProductKey, CurrencyAlternateKey, FIRST_NAME, LAST_NAME, OrderDateKey, OrderQuantity, UnitPrice, SecretCode);
EOF
check_success "Data load step" $?

######################################
# 6. Aktualizacja kolumny SecretCode
######################################

log "Updating SecretCode"

mysql -h "$SQL_HOST" -u "$SQL_USER" -p"$SQL_PASS" "$DB_NAME" <<EOF
UPDATE $TABLE_NAME SET SecretCode = SUBSTRING(MD5(RAND()), 1, 10);
EOF
check_success "SecretCode update step" $?

######################################
# 7. Eksportowanie tabeli do pliku CSV
######################################

log "Exporting table to CSV"

mysql -h "$SQL_HOST" -u "$SQL_USER" -p"$SQL_PASS" --skip-column-names -B "$DB_NAME" \
 -e "SELECT * FROM $TABLE_NAME;" > "$PROCESSED_CSV"
check_success "Export step" $?

######################################
# 8. Kompresowanie pliku CSV
######################################

log "Compressing CSV"

zip "$PROCESSED_ZIP" "$PROCESSED_CSV" > /dev/null
check_success "Compression step" $?

log "Script completed successfully"

