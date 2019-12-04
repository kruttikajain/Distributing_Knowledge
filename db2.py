import redis
import random
from pysyncobj import SyncObj, replicated
from datetime import datetime

r = redis.Redis(host="localhost", port="8101")
#random.seed(444)

class OutOfStockError(Exception):
    """Raised when library is all out of that book"""

class library(SyncObj):
    def __init__(self):
        super().__init__('localhost:4322', ['localhost:4321','localhost:4323','localhost:4324'])


    @replicated
    def init_db(self):
        elements = {"b001":{"book": "Animal Farm", "author": "George Orwell", "year": 1955,"quantity": 4, "nloaned": 0, "type": "Regular"},
                      "b002":{"book": "Harry Potter and the Goblet of Fire","author": "JK Rowling","year": 2000,"quantity": 5,"nloaned": 0,"type": "Reserve"},
                      "b003":{"book": "The Great Gatsby","author": "F. Scott Fitzgerald", "year": 1925,"quantity": 2,"nloaned": 0, "type": "Regular"},
                      "b004":{ "book": "The Hunger Games 1","author": "Suzanne Collins", "year": 2008,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b005":{ "book": "To Kill a Mocking Bird","author": "Harper Lee", "year": 1960,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b006":{ "book": "The Fault In Our Stars","author": "John Green", "year": 2012,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b007":{ "book": "TThe Hobbit","author": "J.R.R Tolkin", "year": 1937,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b008":{ "book": "The Catcher in The Rye","author": "J.D Salinger", "year": 1951,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b009":{ "book": "Angels and Demons","author": "Dan Brown", "year": 2000,"quantity": 3,"nloaned": 0,"type": "Reserved"},
                      "b010":{ "book": "Pride and Prejudice","author": "Jane Austen", "year": 1813,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b011":{ "book": "The Kite Runner","author": "Khalid Hussaini", "year": 2003,"quantity": 3,"nloaned": 0,"type": "Reserved"},
                      "b012":{ "book": "Divergent","author": "Veronica Roth", "year": 2011,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b013":{ "book": "The Diary of a Young Girl","author": "Anne Frank", "year": 1947,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b014":{ "book": "The Girl With The Dragon Tattoo","author": "Steig Larsson", "year": 2005,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b015":{ "book": "Catching Fire","author": "Suzanne Collins", "year": 2009,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b016":{ "book": "Harry Potter and The Prisoner of Azkaban","author": "J.K Rowling", "year": 1999,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b017":{ "book": "The Fellowship of The Ring","author": "J.R.R Tolkin", "year": 1954,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b018":{ "book": "Mockinjay","author": "Suzanne Collins", "year": 2010,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b019":{ "book": "Harry Potter and The Order Of The Phoenix","author": "J.K Rowling", "year": 2003,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b020":{ "book": "The Lovely Bones","author": "Alice Sebold", "year": 2002,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b021":{ "book": "Harry Potter and The Chamber of Secrets","author": "J.K Rowling", "year": 1998,"quantity": 3,"nloaned": 0,"type": "Reserved"},
                      "b022":{ "book": "Harry Potter and The Deathly Hallows","author": "J.K Rowling", "year": 2007,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b023":{ "book": "The Da Vinci Code","author": "Dan Brown", "year": 2003,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b024":{ "book": "Harry Potter and The Half Blood Prince","author": "J.K Rowling", "year": 2005,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b025":{ "book": "Lord of The Flies","author": "William Golding", "year": 1954,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b026":{ "book": "Rome and Juliet","author": "William Shakespeare", "year": 1595,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b027":{ "book": "Gone Girl","author": "Gilian Flynn", "year": 2012,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b028":{ "book": "The Help","author": "Kathryn Strocket", "year": 2009,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b029":{ "book": "Of Mice and Men","author": "John Streinbeck", "year": 1937,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b030":{ "book": "Memoirs of Geisha","author": "Arthur Golden", "year": 1997,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b031":{ "book": "The Alchemist","author": "Paulo Coelho", "year": 1988,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b032":{ "book": "The Giver","author": "Lois Lowry", "year": 1993,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b033":{ "book": "The Lion, the witch and the Wardrobe","author": "C.S Lewis", "year": 1950,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b034":{ "book": "The Time Traveller's wife","author": "Audrey Nifnegg", "year": 1996,"quantity": 3,"nloaned": 0,"type": "Reserved"},
                      "b035":{ "book": "Eat Pray Love","author": "Elizabeth Gilbert", "year": 2006,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b036":{ "book": "The Lightning Theif","author": "Rick Riordon", "year": 2005,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b037":{ "book": "Little Women","author": "Louisa M.A", "year": 1868,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b038":{ "book": "Jane Eyre","author": "Charlotte Bronte", "year": 1847,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b039":{ "book": "The Notebook","author": "Nicholas Sparks", "year": 1996,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b040":{ "book": "Life of Pi","author": "Yann Martel", "year": 2001,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b041":{ "book": "New Moon","author": "Stephnie Meyer", "year": 2006,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b042":{ "book": "Where The Sidewalk Ends","author": "Shel Silverstein", "year": 1974,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b043":{ "book": "City of Bones","author": "Cassandra Clare", "year": 2007,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b044":{ "book": "Eclipse","author": "Stephnie Meyer", "year": 2007,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b045":{ "book": "Eragon","author": "Christopher Paoli", "year": 2002,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b046":{ "book": "The Hitchhiker's Guide to the Galaxy","author": "Douglas Adams", "year": 1979,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b047":{ "book": "Brave New World","author": "Aldous Huxley", "year": 1932,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b048":{ "book": "Breaking Dawn","author": "Stephnie Meyer", "year": 2008,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b049":{ "book": "The Secret Life of Bees","author": "Sue Monk Kidd", "year": 2001,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b050":{ "book": "The Adventures of HuckleBerry Finn","author": "Mark Twain", "year": 1884,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b051":{ "book": "Charlotte's Web","author": "E.B White", "year": 1952,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b052":{ "book": "The Girl on The Train","author": "Paula Hawkins", "year": 2015,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b053":{ "book": "The Golden Compass","author": "Phillip Pullman", "year": 1995,"quantity": 3,"nloaned": 0,"type": "Reserved"},
                      "b054":{ "book": "Wuthering Heights","author": "Emily Bront", "year": 1847,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b055":{ "book": "My sister's Keeper","author": "Jodi Picoult", "year": 2004,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b056":{ "book": "Gone with the wind","author": "Margaret Mitchell", "year": 1936,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b057":{ "book": "Insurgent","author": "Veronica Roth", "year": 2012,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b058":{ "book": "Looking For Alaska","author": "John Green", "year": 2008,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b059":{ "book": "Holes","author": "Louis Sacher", "year": 2006,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b060":{ "book": "The Devil Wears Prada","author": "Lauren Weisberg", "year": 2004,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b061":{ "book": "The Glass Castle","author": "Jeanette Walls", "year": 2005,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b062":{ "book": "A Tale of Two Cities","author": "Charles Dickens", "year": 1859,"quantity": 3,"nloaned": 0,"type": "Reserved"},
                      "b063":{ "book": "The Giving Tree","author": "Shel Silverstein", "year": 2008,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b064":{ "book": "The Princess Bride","author": "William Goldman", "year": 1973,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b065":{ "book": "Dracula","author": "Bram Stoker", "year": 1897,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b066":{ "book": "The Poisonwood Bible","author": "Barbara Kingsolv", "year": 1998,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b067":{ "book": "The Road","author": "Cormac McCarth", "year": 2006,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b068":{ "book": "Where The Wild Things Are","author": "Maurice Sendak", "year": 1963,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b069":{ "book": "A Clash of Kings","author": "George R R Martin", "year": 1998,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b070":{ "book": "Me Before You","author": "Jojo Moyes", "year": 2012,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b071":{ "book": "The Adventures of Tom Sawyer","author": "Mark Twain", "year": 1876,"quantity": 3,"nloaned": 0,"type": "Reserved"},
                      "b072":{ "book": "Tha Handmaid's Tale","author": "Margaret Antwood", "year": 1985,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b073":{ "book": "Lolita","author": "Vladimir Noboklov", "year": 1955,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b074":{ "book": "Hamlet","author": "William Shakespeare", "year": 1600,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b075":{ "book": "The Old man and the Sea","author": "Earnst Hemmingway", "year": 1952,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b076":{ "book": "The Grapes of Wrath","author": "John Steinbeck", "year": 1939,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b077":{ "book": "City of Glass","author": "Cassandra Clare", "year": 2009,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b078":{ "book": "Outlander","author": "Diana Gabaldon", "year": 1996,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b079":{ "book": "Paradise","author": "Judith McNaught", "year": 2004,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b080":{ "book": "Dune","author": "Frank Herbert", "year": 1965,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b081":{ "book": "The scarlet Letter","author": "Nathaniel Hawthorne", "year": 1850,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b082":{ "book": "The Martian","author": "Andy Weir", "year": 2012,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b083":{ "book": "Deception Point","author": "Dan Brown", "year": 2001,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b084":{ "book": "Dear John","author": "Nicholas Sparks", "year": 2006,"quantity": 3,"nloaned": 0,"type": "Reserved"},
                      "b085":{ "book": "Something Borrowed","author": "Emily Giffin", "year": 2004,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b086":{ "book": "The Battle of the Labyrinth","author": "Rick Riordon", "year": 2006,"quantity": 3,"nloaned": 0,"type": "Reserved"},
                      "b087":{ "book": "If I stay","author": "Gayle Forman", "year": 2009,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b088":{ "book": "The Stranger","author": "Alburt Camus", "year": 1942,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b089":{ "book": "The Stand","author": "Stephen King", "year": 1978,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b090":{ "book": "A Clockwork Orange","author": "Anthony Burges", "year": 1962,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b091":{ "book": "The Bell Jar","author": "Sylvia Plath", "year": 1963,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b092":{ "book": "Matilda","author": "Roald Dahl", "year": 1988,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b093":{ "book": "And Then There Were None","author" : "Agatha Christie", "year": 1939,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b094":{ "book": "The Selection","author": "Kiera Cass", "year": 2012,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b095":{ "book": "The Silence of the Lambs","author": "Thomas Harris", "year": 1988,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b096":{ "book": "Atonement","author": "Ian McEwan", "year": 2001,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b097":{ "book": "The Bourne Identity","author": "Robert Ludium", "year": 1980,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b098":{ "book": "Speak","author": "Laurie Halse", "year": 1999,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b099":{ "book": "Number the Stars","author": "Lois Lowry", "year": 1989,"quantity": 3,"nloaned": 0,"type": "Regular"},
                      "b100":{ "book": "Under the Tuscan Sun","author": "Frances Mayes", "year": 1996,"quantity": 3,"nloaned": 0,"type": "Regular"},


                      "s001":{"Name": "Andrew","bookloaned": " ", "returndate": 0},
                      "s002":{"Name": "Bill","bookloaned":" ", "returndate": 0},
                      "s003":{"Name": "Cristy","bookloaned":" ","returndate": 0},
                      "s004":{"Name": "Derek","bookloaned":" ","returndate": 0},
                      "s005":{"Name": "Shanice","bookloaned":" ","returndate": 0},
                      "s006":{"Name": "Melany","bookloaned":" ","returndate": 0},
                      "s007":{"Name": "Wayne","bookloaned":" ","returndate": 0},
                      "s008":{"Name": "Ilona","bookloaned":" ","returndate": 0},
                      "s009":{"Name": "Marylyn","bookloaned":" ","returndate": 0},
                      "s010":{"Name": "Donette","bookloaned":" ","returndate": 0},
                      "s011":{"Name": "Maire","bookloaned":" ","returndate": 0},
                      "s012":{"Name": "Veronica","bookloaned":" ","returndate": 0},
                      "s013":{"Name": "Shelby","bookloaned":" ","returndate": 0},
                      "s014":{"Name": "Tonita","bookloaned":" ","returndate": 0},
                      "s015":{"Name": "Allie","bookloaned":" ","returndate": 0},
                      "s016":{"Name": "Leslie","bookloaned":" ","returndate": 0},
                      "s017":{"Name": "Rhea","bookloaned":" ","returndate": 0},
                      "s018":{"Name": "Lila","bookloaned":" ","returndate": 0},
                      "s019":{"Name": "Sammy","bookloaned":" ","returndate": 0},
                      "s020":{"Name": "Vanita","bookloaned":" ","returndate": 0},
                      "s021":{"Name": "Jane","bookloaned":" ","returndate": 0},
                      "s023":{"Name": "Jarret","bookloaned":" ","returndate": 0},
                      "s024":{"Name": "Dwain","bookloaned":" ","returndate": 0},
                      "s025":{"Name": "Antonia","bookloaned":" ","returndate": 0},
                      "s026":{"Name": "Mercy","bookloaned":" ","returndate": 0},
                      "s027":{"Name": "Kristine","bookloaned":" ","returndate": 0},
                      "s028":{"Name": "Keith","bookloaned":" ","returndate": 0},
                      "s029":{"Name": "Dante","bookloaned":" ","returndate": 0},
                      "s030":{"Name": "Sharyn","bookloaned":" ","returndate": 0},
                      "s031":{"Name": "Cleora","bookloaned":" ","returndate": 0},
                      "s032":{"Name": "Bryan","bookloaned":" ","returndate": 0},
                      "s033":{"Name": "Lacie","bookloaned":" ","returndate": 0},
                      "s034":{"Name": "Ned","bookloaned":" ","returndate": 0},
                      "s035":{"Name": "Alease","bookloaned":" ","returndate": 0},
                      "s036":{"Name": "Kaitlin","bookloaned":" ","returndate": 0},
                      "s037":{"Name": "Laticia","bookloaned":" ","returndate": 0},
                      "s038":{"Name": "Kacey","bookloaned":" ","returndate": 0},
                      "s039":{"Name": "Delphine","bookloaned":" ","returndate": 0},
                      "s040":{"Name": "Andy","bookloaned":" ","returndate": 0},
                      "s041":{"Name": "Lucas","bookloaned":" ","returndate": 0},
                      "s042":{"Name": "Keith","bookloaned":" ","returndate": 0},
                      "s043":{"Name": "Nathan","bookloaned":" ","returndate": 0},
                      "s044":{"Name": "Brook","bookloaned":" ","returndate": 0},
                      "s045":{"Name": "Haley","bookloaned":" ","returndate": 0},
                      "s046":{"Name": "Peyton","bookloaned":" ","returndate": 0},
                      "s047":{"Name": "Dan","bookloaned":" ","returndate": 0},
                      "s048":{"Name": "Antwon","bookloaned":" ","returndate": 0},
                      "s049":{"Name": "Jamie","bookloaned":" ","returndate": 0},
                      "s050":{"Name": "Lydia","bookloaned":" ","returndate": 0},
                      "s051":{"Name": "Marvin","bookloaned":" ","returndate": 0},
                      "s052":{"Name": "Millie","bookloaned":" ","returndate": 0},
                      "s053":{"Name": "Miley","bookloaned":" ","returndate": 0},
                      "s054":{"Name": "Selena","bookloaned":" ","returndate": 0},
                      "s055":{"Name": "Serena","bookloaned":" ","returndate": 0},
                      "s056":{"Name": "Meredith","bookloaned":" ","returndate": 0},
                      "s057":{"Name": "Alex","bookloaned":" ","returndate": 0},
                      "s058":{"Name": "Richard","bookloaned":" ","returndate": 0},
                      "s059":{"Name": "Bailey","bookloaned":" ","returndate": 0},
                      "s060":{"Name": "Harry","bookloaned":" ","returndate": 0},
                      "s061":{"Name": "James","bookloaned":" ","returndate": 0},
                      "s062":{"Name": "Lily","bookloaned":" ","returndate": 0},
                      "s063":{"Name": "Fern","bookloaned":" ","returndate": 0},
                      "s064":{"Name": "Lionel","bookloaned":" ","returndate": 0},
                      "s065":{"Name": "Antonella","bookloaned":" ","returndate": 0},
                      "s066":{"Name": "Thiago","bookloaned":" ","returndate": 0},
                      "s067":{"Name": "Mateo","bookloaned":" ","returndate": 0},
                      "s068":{"Name": "Nichole","bookloaned":" ","returndate": 0},
                      "s069":{"Name": "Chuck","bookloaned":" ","returndate": 0},
                      "s070":{"Name": "Florence","bookloaned":" ","returndate": 0},
                      "s071":{"Name": "Lorelai","bookloaned":" ","returndate": 0},
                      "s072":{"Name": "Chris","bookloaned":" ","returndate": 0},
                      "s073":{"Name": "Vivien","bookloaned":" ","returndate": 0},
                      "s074":{"Name": "George","bookloaned":" ","returndate": 0},
                      "s075":{"Name": "Lorine","bookloaned":" ","returndate": 0},
                      "s076":{"Name": "Ross","bookloaned":" ","returndate": 0},
                      "s077":{"Name": "Rachel","bookloaned":" ","returndate": 0},
                      "s078":{"Name": "Joey","bookloaned":" ","returndate": 0},
                      "s079":{"Name": "Phoebe","bookloaned":" ","returndate": 0},
                      "s080":{"Name": "Chandler","bookloaned":" ","returndate": 0},
                      "s081":{"Name": "Monica","bookloaned":" ","returndate": 0},
                      "s082":{"Name": "Macy","bookloaned":" ","returndate": 0},
                      "s083":{"Name": "Zara","bookloaned":" ","returndate": 0},
                      "s084":{"Name": "Naina","bookloaned":" ","returndate": 0},
                      "s085":{"Name": "Jeffrey","bookloaned":" ","returndate": 0},
                      "s086":{"Name": "Meg","bookloaned":" ","returndate": 0},
                      "s087":{"Name": "Taylor","bookloaned":" ","returndate": 0},
                      "s088":{"Name": "Katy","bookloaned":" ","returndate": 0},
                      "s089":{"Name": "Fred","bookloaned":" ","returndate": 0},
                      "s090":{"Name": "Percy","bookloaned":" ","returndate": 0},
                      "s091":{"Name": "Charlie","bookloaned":" ","returndate": 0},
                      "s092":{"Name": "Bill","bookloaned":" ","returndate": 0},
                      "s093":{"Name": "Seamus","bookloaned":" ","returndate": 0},
                      "s094":{"Name": "Lloyd","bookloaned":" ","returndate": 0},
                      "s095":{"Name": "Rose","bookloaned":" ","returndate": 0},
                      "s096":{"Name": "Edmund","bookloaned":" ","returndate": 0},
                      "s097":{"Name": "Nate","bookloaned":" ","returndate": 0},
                      "s098":{"Name": "Eugene","bookloaned":" ","returndate": 0},
                      "s099":{"Name": "Wendy","bookloaned":" ","returndate": 0},
                      "s100":{"Name": "Jennifer","bookloaned":" ","returndate": 0}
                      
                    }

        with r.pipeline() as pipe:
            for h_id, book in elements.items():
                pipe.hmset(h_id, book)
            pipe.execute()


        r.bgsave()
        print (elements)
        print(r.keys())

    @replicated    
    def loanitem(self, itemid: int, studentid: int) -> None:
        with open('log2.txt', 'a') as logfile:
            logfile.write(datetime.now().strftime('loanitem start %H:%M:%S.%f\n'))
        with r.pipeline() as pipe:
            error_count = 0
            while True:
                try:
                                 
                    pipe.watch(itemid)                   
                    nleft: bytes = r.hget(itemid, "quantity")                
                    nbook: bytes = r.hget(itemid, "book")   
                  
                  
                    if nleft > b"0":
                        pipe.multi()
                        pipe.hincrby(itemid, "quantity", -1)
                        pipe.hincrby(itemid, "nloaned", 1)
                        pipe.hset(studentid, "bookloaned", nbook)
                        pipe.execute()                    
                        break
                    else:
                        
                        pipe.unwatch()
                        raise OutOfStockError(
                            f"Sorry,  the book is not available at the moment!"
                        )
                except redis.WatchError:
                    
                    error_count += 1
                    logging.warning(
                        "WatchError #%d: %s; retrying",
                        error_count, itemid
                    )
        with open('log2.txt', 'a') as logfile:
            logfile.write(datetime.now().strftime('loanitem end %H:%M:%S.%f\n'))
        return None
    
    @replicated
    def returnitem(self, itemid: int, studentid: int) -> None:
      with r.pipeline() as pipe:
          error_count = 0
          while True:
              try:
                                 
                  pipe.watch(itemid)                   
                  nleft: bytes = r.hget(itemid, "nloaned")                
                  nbook: bytes = r.hget(itemid, "book")                 
                  
                  if nleft > b"0":
                      pipe.multi()
                      pipe.hincrby(itemid, "quantity", 1)
                      pipe.hincrby(itemid, "nloaned", -1)
                      pipe.hset(studentid, "bookloaned", "")
                      pipe.execute()                    
                      break
                  else:                    
                      pipe.unwatch()
                      raise OutOfStockError(
                          f"Check the book code!"
                      )
              except redis.WatchError:                
                  error_count += 1
                  logging.warning(
                      "WatchError #%d: %s; retrying",
                      error_count, itemid
                  )
      return None   



#import pdb; pdb.set_trace()
if __name__ == '__main__':
    from time import sleep
    obj = library()
    #obj.init_db()
    sleep(5)
    obj.loanitem("b001", "s005")
    sleep(1.2)

