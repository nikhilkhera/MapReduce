#commands to write in commandline
#if we want to reuse our cluster
# mrjob create-cluster --conf-path=mrjob.conf
# python MovieSimilarities.py -r emr --cluster-id=j-CLUSTERID --items=u.item u.data > sims.txt
# else
# python MovieSimilarities.py -r emr --conf-path=mrjob.conf --items=u.item u.data > sims.txt
from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt
from itertools import combinations

##The mapreduce class
class MovieSimilarities(MRJob):

	##Fucntion to set up mapreduce configuration
    def configure_args(self):
	
        super(MovieSimilarities, self).configure_args()
		
		##Add file option to tell there is another auxillary file that we want to send to the MapReduce job
        self.add_file_arg('--items', help='Path to u.item')

	##Fucntion to load database of movie id and corresponding movie name from the file "u.item"
    def load_movie_names(self):
	
        #A dictionary to store the movie id and movie name as key/value pairs
        self.movieNames = {}

		##Opeining the file for reading
        with open("u.item") as f:
		
			##Iterating through every line of the file
            for line in f:
			
				##Splitting the pip delimited data in the line
                fields = line.split('|')
				
				##Inserting into the dictionary the movie id and corresponding movie name as key/value pairs
                self.movieNames[int(fields[0])] = fields[1]

	##Function to define the multistep MapReduce job
    def steps(self):
        return [
		
            MRStep(mapper=self.mapper_parse_input, ##Mapper and Reducer functions of 1st step of the MapReduce job i.e to create movie rating pair by user
                    reducer=self.reducer_ratings_by_user),
            MRStep(mapper=self.mapper_create_item_pairs, ##Mapper and Reducer functions of 2nd step of the MapReduce job	
                    reducer=self.reducer_compute_similarity),
            MRStep(mapper=self.mapper_sort_similarities, ##Mapper and Reducer functions of 3rd step of the MapReduce job	
                    mapper_init=self.load_movie_names,
                    reducer=self.reducer_output_similarities)]

	##Mapper function parses the input from the file given in the command line
    def mapper_parse_input(self, key, line):
	
        ##Splitting the tab delimited data in the line and storing them in the appropriate variables
        (userID, movieID, rating, timestamp) = line.split('\t')
		
		##Yielding the userID, and a tupl of movieID, rating as key/value pair
        yield  userID, (movieID, float(rating))

	##Function to group (item, rating) pairs by userID
    def reducer_ratings_by_user(self, user_id, itemRatings):
        
		##A list to store the ratings
        ratings = []
		
		##Iterating through every movieID & rating in itemRatings of a user under consideration
        for movieID, rating in itemRatings:
		
			##Append the tupl of movie ID, rating to the list of rating
            ratings.append((movieID, rating))

		##Yielding the userID, and the list of the above tupls
        yield user_id, ratings

	##Mapper function to find every pair of movies each user has seen,
    #and yield each pair with its associated ratings
    def mapper_create_item_pairs(self, user_id, itemRatings):
        
		##"combinations" finds every possible pair from the list of movies
        # this user viewed.
        for itemRating1, itemRating2 in combinations(itemRatings, 2):
		
			##Extracting the movie ids and the ratings of the pair 
            movieID1 = itemRating1[0]
            rating1 = itemRating1[1]
            movieID2 = itemRating2[0]
            rating2 = itemRating2[1]

            # Produce both orders so sims are bi-directional
            yield (movieID1, movieID2), (rating1, rating2)
            yield (movieID2, movieID1), (rating2, rating1)


	##A function to compute and return the cosine similarity metric between two rating vectors.
    def cosine_similarity(self, ratingPairs):
        
		##Variable to store the total number of pairs 
        numPairs = 0
		
		##Computing the similarity metric
        sum_xx = sum_yy = sum_xy = 0
        for ratingX, ratingY in ratingPairs:
            sum_xx += ratingX * ratingX
            sum_yy += ratingY * ratingY
            sum_xy += ratingX * ratingY
            numPairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))
			
			
		##Return a tupl of cosine similarrity score and number of co-ratings
        return (score, numPairs)

	##Reducer function to compute the similarity score between the ratings vectors
    #for each movie pair viewed by multiple people
    def reducer_compute_similarity(self, moviePair, ratingPairs):
	
        #Calling the cosine_similarity function to retrieve the score, number of co-ratings
        score, numPairs = self.cosine_similarity(ratingPairs)

        # Enforce a minimum score and minimum number of co-ratings
        # to ensure quality
        if (numPairs > 10 and score > 0.95):
		
			##Yielding moviePair and a tupl of similarity soore and the number of co-ratings 
            yield moviePair, (score, numPairs)

	##A fucntion to shuffle things around so the key is (movie1, score)
    #so we have meaningfully sorted results.
    def mapper_sort_similarities(self, moviePair, scores):
	
        ##Extract the similarity metric and number of co-ratings of movie pair under consideration
        score, n = scores
		
		##Extracting the movie pairs 
        movie1, movie2 = moviePair

		##yielding the following two tupls as the key/value pairs
        yield (self.movieNames[int(movie1)], score), \
            (self.movieNames[int(movie2)], n)

	##Output the results in the follwoing key/value format
    #Movie => Similar Movie, score, number of co-ratings
    def reducer_output_similarities(self, movieScore, similarN):
        #Extracting the name of movie1 and score 
        movie1, score = movieScore		
		
        for movie2, n in similarN:
            yield movie1, (movie2, score, n)

##The main method of the sourcecode		
if __name__ == '__main__':

	##Running the map reduce class above
    MovieSimilarities.run()