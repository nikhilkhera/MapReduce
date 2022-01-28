from mrjob.job import MRJob
from mrjob.step import MRStep

class StarWarsSims(MRJob):

    def steps(self):
        return [
		
            MRStep(mapper=self.mapper_movies, ##Mapper and Reducer functions of 1st step of the MapReduce job i.e to create movie rating pair by user
                    reducer=self.reducer_movies)
          ]

    def mapper_movies(self, key, line):
        (movieName, value) = line.split('\t')
        if (movieName == '"Batman & Robin (1997)"'):
            (bracket, simName, values) = value.split('"')
            (empty, score, coraters) = values.split(',')
            cleanCoraters = coraters.split(']')
            numCoraters = int(cleanCoraters[0])
            if (numCoraters > 15):
                yield float(score), (simName, numCoraters)
        
    def reducer_movies(self, key, values):
        for value in values:
            yield key, value
        
if __name__ == '__main__':
    StarWarsSims.run()
    