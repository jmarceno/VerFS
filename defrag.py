


def defrag(trim=True, trim_percent=.8):
    """
    :param trim: Specify if the datastore should be trimmed after the defrag process to release physical space
    :param trim_percent: Percent (decimal notation) that should be trimmed. Default is .8, leaving 20% space for the datastore to grow without the need to extra allocation
    """
    global allocation_unit
    global free_blocks
    global datastore
    global hash_table
    global key_index
    global write_lock

    trim_size = 1

    timer = Timer()
    timer.start()
    if len(free_blocks) > 0:
        print("Starting Defrag - Expect some slowdown")
    write_lock = True

    fb_copy = free_blocks.copy()
    for fb in fb_copy:
        try:
            last_block = max(hash_table.items(), key=operator.itemgetter(1))[1]  # Encontrar o bloco de valor mais alto
            datastore.seek(last_block)  # Mover cursor para o último bloco
            data = datastore.read(allocation_unit)  # Ler último bloco
            data_hash = hashlib.sha3_256(data).hexdigest()  # Calcular hash do ultimo bloco
            datastore.seek(fb)  # Mover cursor até o primeiro bloco livre
            datastore.write(data)  # Escrever dados no primeiro bloco livre
            hash_table[data_hash] = fb  # Atulizar hash_table com a nova posição do bloco
            # free_blocks.pop(0)  # Remover o primeiro bloco da lista de blocos livres
            free_blocks.remove(fb)
            trim_size = trim_size + 1  # Incrementa marcador para calculo do tamanho do arquivo a ser reduzido

            # if len(free_blocks) == 0:
            #     break

        except Exception as ex:
            print(traceback.format_exc())
            continue

        # last_block = max(hash_table.items(), key=operator.itemgetter(1))[1]  # Encontrar o bloco de valor mais alto
        # try:
        #     idx = free_blocks.index(last_block)  # Checa se o último bloco esta livre
        #     free_blocks.pop(idx)  # Remove o último bloco da lista de blocos livres
        #     trim_size = trim_size + 1  # Incrementa marcador para calculo do tamanho do arquivo a ser reduzido
        # except IndexError:
        #     print("\nIndex controlling the free blocks has changed during execution\n")
        #     print(traceback.format_exc())
        # except ValueError:
        #     for fb in free_blocks:
        #         try:
        #             # datastore.seek(datastore.size() - (allocation_unit*trim_size) - 1)  # Mover cursor para o último bloco
        #             datastore.seek(last_block)  # Mover cursor para o último bloco
        #             # free_blocks.append(datastore.tell())  # Ele agora vai ser vago entao adicionar ao freeblocks
        #             data = datastore.read(allocation_unit)  # Ler último bloco
        #             data_hash = hashlib.sha3_256(data).hexdigest()  # Calcular hash do ultimo bloco
        #             datastore.seek(fb)  # Mover cursor até o primeiro bloco livre
        #             datastore.write(data)  # Escrever dados no primeiro bloco livre
        #
        #             hash_table[data_hash] = fb  # Atulizar hash_table com a nova posição do bloco
        #             # update_index(data_hash, False)  # Remove bloco do key_index para que ele não conste mais durante a atualização dos blocos livres
        #             # update_index(fb)  # Atualiza index para constar que o bloco agora esta novamente em utilização
        #             free_blocks.pop(0)  # Remover o primeiro bloco da lista de blocos livres
        #
        #             trim_size = trim_size + 1  # Incrementa marcador para calculo do tamanho do arquivo a ser reduzido
        #
        #             if len(free_blocks) == 0:
        #                 break
        #         except Exception as ex:
        #             print(traceback.format_exc())
        #             break
        time.sleep(defrag_internal_interval)
    timer.stop()
    if trim_size > 1:
        print("Defrag Ended. Time Taken: " + str(timer.elapsed()))

    # if trim and trim_size > 1:
    #     print("Staring trimming process. Expect some slowdown")
    #     timer = Timer()
    #     timer.start()
    #     datastore.resize(int(max(hash_table.items(), key=operator.itemgetter(1))[1]+allocation_unit+1))
    #     timer.stop()
    #     print("Finished trimming process. Time Taken: " + str(timer.elapsed()))

    write_lock = False