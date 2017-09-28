#!/usr/bin/env python3

import logging
import os
import sys

from medicine import downloadCatalogo
from medicine import downloadFarmsInfo
from medicine import downloadFarmsInfo2
from medicine import downloadProdFarm
from medicine import downloadProdsInfo
from medicine import downloadSugesstions
from medicine import uploadCatalogo
from medicine import uploadFarms
from medicine import uploadProds
from medicine import uploadProdsFarms

files = {"catalog": 'data/products.csv',
         'suggestion': 'data/suggestions.json',
         'prod_farm': 'data/prod_farm.json',
         'farms': 'data/farms.json',
         'farms2': 'data/farms2.json',
         'prods': 'data/prods.json'}


def mv_files():
    for k, v in files.items():
        os.rename(v, v + '.bak')


def main(nthreads=15):
    if not os.path.exists(files['suggestion']):
        downloadSugesstions.download(files['suggestion'])
    downloadProdFarm.download(nthreads, files['suggestion'], files['prod_farm'])
    if not os.path.exists(files['farms']):
        downloadFarmsInfo.download(nthreads, files['prod_farm'], files['farms'])
    if not os.path.exists(files['farms2']):
        downloadFarmsInfo2.download(nthreads, files['farms'], files['farms2'])
    if not os.path.exists(files['prods']):
        downloadProdsInfo.download(nthreads, files['prod_farm'], files['prods'])

    if not downloadCatalogo.download(files['catalog']):
        logging.critical('Does not possible to download the catalog.')
        return

    logging.info('Uploading Catalogo')
    uploadCatalogo.upload(files['catalog'])

    logging.info('Uploading Products Aditional Info')
    uploadProds.upload(files['prods'])

    logging.info('Uploading Pharmacies')
    uploadFarms.upload(files['prod_farm'], 0)
    uploadFarms.upload(files['farms'], 1)
    uploadFarms.upload(files['farms2'], 2)

    logging.info('Uploading Products Pharmacies Prices')
    uploadProdsFarms.upload(files['prod_farm'])

    logging.info('Moving tempory files.')
    mv_files()


if __name__ == '__main__':
    logging.basicConfig(filename='logging.log', level=logging.DEBUG,
                        format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
    if len(sys.argv) == 1:
        main()
    else:
        main(int(sys.argv[1]))
