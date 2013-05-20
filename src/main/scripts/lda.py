import logging, gensim, bz2, sys
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
id2word = gensim.corpora.wikicorpus.WikiCorpus.loadDictionary(sys.argv[1] + '_wordids.txt')
mm = gensim.corpora.MmCorpus(sys.argv[1] + '_tfidf.mm')
lda = gensim.models.ldamodel.LdaModel(corpus=mm, id2word=id2word, numTopics=100, update_every=1, chunks=10000, passes=1)
