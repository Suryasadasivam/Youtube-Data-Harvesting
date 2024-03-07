
YouTube Data Harvesting and Warehousing is a project aimed at enabling users to access and analyze data from various YouTube channels. It utilizes SQL, MongoDB, and Streamlit to develop a user-friendly application for retrieving, saving, and querying YouTube channel and video data.

**Tools and Libraries Used:**

Streamlit: This library is employed to create a user-friendly UI, allowing users to interact with the program for data retrieval and analysis.

Python: Python serves as the primary programming language for the project, facilitating the development of the entire application, including data retrieval, processing, analysis, and visualization.

Google API Client: The googleapiclient library in Python facilitates communication with various Google APIs, primarily YouTube's Data API v3. It enables the retrieval of essential information such as channel details, video specifics, and comments.

MongoDB Atlas: MongoDB Atlas, a cloud-based database service, is used to store data obtained from YouTube's Data API v3. It offers a managed and scalable database solution for efficient data management.

MYSQL: MySQL, an open-source DBMS, is utilized for storing and managing structured data. It provides advanced SQL capabilities and ensures reliability and scalability.

YouTube Data Scraping and Ethical Perspective:
When scraping YouTube content, it's essential to do so ethically and responsibly, adhering to YouTube's terms and conditions, obtaining proper authorization, and complying with data protection regulations. The collected data should be handled responsibly to ensure privacy and prevent misuse. Additionally, considering the impact on the platform and its community is crucial for a fair and sustainable scraping process.

**Required Libraries:**

- googleapiclient.discovery
- streamlit
- sqlalchemy
- pymongo
- pandas


**Features:**
The YouTube Data Harvesting and Warehousing application offers the following functions:

Retrieval of channel and video data from YouTube using the YouTube API.
Storage of data in a MongoDB database.
Migration of data from MongoDB to a SQL database for efficient querying and analysis.
Search and retrieval of data from the SQL database using various search options.
