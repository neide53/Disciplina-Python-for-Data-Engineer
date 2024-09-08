'''
O Código Morse é um sistema de representação de letras, algarismos e sinais de pontuação através
de um sinal codificado enviado de modo intermitente. Foi desenvolvido por Samuel Morse em 1837, 
criador do telégrafo elétrico, dispositivo que utiliza correntes elétricas para controlar eletroímãs 
que atuam na emissão e na recepção de sinais. 
O script tem a finalidade de decifrar uma mensagem em código morse e salvá-la em texto claro.
'''

import os
import sys
import datetime
import pandas as pd
from config import file_path, dict_morse

def decode_morse(msg):
    '''
    input: mensagem em código morse com as letras separadas por um espaço com as palavras separadas por dois espaços
    output: mensagem escrita em letras e algarismos
    '''
    word_list = msg.split("  ")  # Palavras separadas por dois espaços
    decoded_message = []

    for word in word_list:
        letters_list = word.split(" ")  # Letras separadas por um espaço
        decoded_letters = []
        
        for letter in letters_list:
            decoded_letter = dict_morse.get(letter, '')  # Decodifica cada letra
            decoded_letters.append(str(decoded_letter))  # Garante que é uma string

        # Junta as letras para formar uma palavra
        decoded_word = "".join(decoded_letters)
        decoded_message.append(decoded_word)  # Adiciona a palavra completa à lista final
        
    return " ".join(decoded_message)  # Unir palavras com um espaço

def save_clear_msg_csv_hdr(msg_claro):
    '''
    input : mensagem em texto claro
    output : palavra escrito em letras e algarismos, salva em arquivo csv
    '''
    now = datetime.datetime.now()
    df = pd.DataFrame([[msg_claro, now]], columns=["mensagem", "datetime"])
    hdr = not os.path.exists(file_path)
    df.to_csv(file_path, mode ="a", index = False, header=hdr)

if __name__ == "__main__":
    msg_claro = decode_morse(sys.argv[1])
    print (msg_claro)
    #save_clear_msg_csv_hdr(msg_claro)
    #print(save_clear_msg_csv_hdr.__doc__)
    #print(pd.to_pickle.__doc__)