using Npgsql;

namespace Paper_By_Country_Consumer
{
    public static class DbHelper
    {
        private static string _connectionString = "Host=host.docker.internal;Port=5432;Username=postgres;Password=admin;Database=sfr2025";

        public static void EnsureTableExists()
        {
            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            using var cmd = new NpgsqlCommand(@"
                CREATE TABLE IF NOT EXISTS papers (
                    id INT PRIMARY KEY,
                    name TEXT,
                    authors TEXT[],
                    keywords TEXT[],
                    countryofpublication TEXT
                );", conn);

            cmd.ExecuteNonQuery();
        }

        public static void SavePaper(Paper paper)
        {
            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();

            using var cmd = new NpgsqlCommand(@"
                INSERT INTO papers (id, name, authors, keywords, countryofpublication)
                VALUES (@id, @name, @authors, @keywords, @country)
                ON CONFLICT (id) DO NOTHING;", conn);

            cmd.Parameters.AddWithValue("id", paper.Id);
            cmd.Parameters.AddWithValue("name", paper.Name);
            cmd.Parameters.AddWithValue("authors", paper.Authors.ToArray());
            cmd.Parameters.AddWithValue("keywords", paper.Keywords.ToArray());
            cmd.Parameters.AddWithValue("country", paper.CountryOfPublication);

            cmd.ExecuteNonQuery();
        }
    }
}
