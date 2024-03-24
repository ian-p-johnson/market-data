#!/bin/bash

echo "download = $*"

java -cp  build/libs/market-data-1.0-SNAPSHOT.jar com.flash.data.tardis.TardisDownloader \
-a $API_TARDIS \
-type quotes,trades,incremental_book_L2 \
-t 6 \
-ref /mnt/nvme_4tb/datasets \
-tmp /mnt/nvme_4tb/tmp -dest /mnt/nvme_4tb/datasetsz \
-from - -to 2024/02/01 $*