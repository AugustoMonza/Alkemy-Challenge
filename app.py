import logging
import source_files
import norm

def unique():
    logging.basicConfig(filename='app.log',
                        level=logging.INFO,
                        format='%(levelname)s: %(message)s')
    logging.info('Inicio de descarga')
    source_files.files()
    logging.info('Normalizado de las tablas')
    norm.main()
    logging.info('Challenge realizado por Augusto Damian Monza')
    
if __name__ == '__main__':
    unique()