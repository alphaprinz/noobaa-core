#include "../../../submodules/s3select/include/s3select.h"
#include <string>

namespace s3selectEngine {

class ParquetStreamParser
{

}

class parquet_file_parser
{

public:

  typedef std::vector<std::pair<std::string, column_reader_wrap::parquet_type>> schema_t;
  typedef std::set<uint16_t> column_pos_t;
  typedef std::vector<column_reader_wrap::parquet_value_t> row_values_t;

  typedef column_reader_wrap::parquet_value_t parquet_value_t;
  typedef column_reader_wrap::parquet_type parquet_type;

private:

  std::string m_parquet_file_name;
  uint32_t m_num_of_columms;
  uint64_t m_num_of_rows;
  uint64_t m_rownum;
  schema_t m_schm;
  int m_num_row_groups;
  std::shared_ptr<parquet::FileMetaData> m_file_metadata;
  std::unique_ptr<parquet::ceph::ParquetFileReader> m_parquet_reader;
  std::vector<column_reader_wrap*> m_column_readers;
  s3selectEngine::rgw_s3select_api* m_rgw_s3select_api;

  public:

  parquet_file_parser(std::string parquet_file_name,s3selectEngine::rgw_s3select_api* rgw_api) : 
                                   m_parquet_file_name(parquet_file_name),
                                   m_num_of_columms(0),
                                   m_num_of_rows(0),
                                   m_rownum(0),
                                   m_num_row_groups(0),
                                   m_rgw_s3select_api(rgw_api)
                                   
                                   
  {
    load_meta_data();
  }

  ~parquet_file_parser()
  {
    for(auto r : m_column_readers)
    {
      delete r;
    }
  }

  int load_meta_data()
  {
    m_parquet_reader = parquet::ceph::ParquetFileReader::OpenFile(m_parquet_file_name,m_rgw_s3select_api,false);
    m_file_metadata = m_parquet_reader->metadata();
    m_num_of_columms = m_parquet_reader->metadata()->num_columns();
    m_num_row_groups = m_file_metadata->num_row_groups();
    m_num_of_rows = m_file_metadata->num_rows();

    for (uint32_t i = 0; i < m_num_of_columms; i++)
    {
      parquet::Type::type tp = m_file_metadata->schema()->Column(i)->physical_type();
      std::pair<std::string, column_reader_wrap::parquet_type> elm;

      switch (tp)
      {
      case parquet::Type::type::INT32:
        elm = std::pair<std::string, column_reader_wrap::parquet_type>(m_file_metadata->schema()->Column(i)->name(), column_reader_wrap::parquet_type::INT32);
        m_schm.push_back(elm);
        break;

      case parquet::Type::type::INT64:
        elm = std::pair<std::string, column_reader_wrap::parquet_type>(m_file_metadata->schema()->Column(i)->name(), column_reader_wrap::parquet_type::INT64);
        m_schm.push_back(elm);
        break;

      /*case parquet::Type::type::FLOAT:
        elm = std::pair<std::string, column_reader_wrap::parquet_type>(m_file_metadata->schema()->Column(i)->name(), column_reader_wrap::parquet_type::FLOAT);
        m_schm.push_back(elm);
        break;*/

      case parquet::Type::type::DOUBLE:
        elm = std::pair<std::string, column_reader_wrap::parquet_type>(m_file_metadata->schema()->Column(i)->name(), column_reader_wrap::parquet_type::DOUBLE);
        m_schm.push_back(elm);
        break;

      case parquet::Type::type::BYTE_ARRAY:
        elm = std::pair<std::string, column_reader_wrap::parquet_type>(m_file_metadata->schema()->Column(i)->name(), column_reader_wrap::parquet_type::STRING);
        m_schm.push_back(elm);
        break;

      default:
        {
        std::stringstream err;
        err << "some parquet type not supported";
        throw std::runtime_error(err.str());
        }
      }

      m_column_readers.push_back(new column_reader_wrap(m_parquet_reader,i));
    }

    return 0;
  }

  bool end_of_stream()
  {

    if (m_rownum > (m_num_of_rows-1))
      return true;
    return false;
  }

  uint64_t get_number_of_rows()
  {
    return m_num_of_rows;
  }

  uint64_t rownum()
  {
    return m_rownum;
  }

  bool increase_rownum()
  {
    if (end_of_stream())
      return false;

    m_rownum++;
    return true;
  }

  uint64_t get_rownum()
  {
    return m_rownum;
  }

  uint32_t get_num_of_columns()
  {
    return m_num_of_columms;
  }

  int get_column_values_by_positions(column_pos_t positions, row_values_t &row_values)
  {
    column_reader_wrap::parquet_value_t column_value;
    row_values.clear();

    for(auto col : positions)
    {
      if((col)>=m_num_of_columms)
      {//TODO should verified upon syntax phase 
        //TODO throw exception
        return -1;
      }
      auto status = m_column_readers[col]->Read(m_rownum,column_value);
      if(status == column_reader_wrap::parquet_column_read_state::PARQUET_OUT_OF_RANGE) return -1;
      row_values.push_back(column_value);//TODO intensive (should move)
    }
    return 0;
  }

  schema_t get_schema()
  {
    return m_schm;
  }
};

class nb_parquet_object : public base_s3object
{

private:
  std::string m_error_description;
  parquet_file_parser* object_reader;
  parquet_file_parser::column_pos_t m_where_clause_columns;
  parquet_file_parser::column_pos_t m_projections_columns;
  std::vector<parquet_file_parser::parquet_value_t> m_predicate_values;
  std::vector<parquet_file_parser::parquet_value_t> m_projections_values;
  bool not_to_increase_first_time;

public:

  nb_parquet_object(std::string parquet_file_name, s3select *s3_query,s3selectEngine::rgw_s3select_api* rgw) : base_s3object(s3_query),object_reader(nullptr)
  {
    try{
    
      object_reader = new parquet_file_parser(parquet_file_name,rgw); //TODO uniq ptr
    } catch(std::exception &e)
    { 
      throw base_s3select_exception(std::string("failure while processing parquet meta-data ") + std::string(e.what()) ,base_s3select_exception::s3select_exp_en_t::FATAL);
    }

    parquet_query_setting(nullptr);
  }

  nb_parquet_object() : base_s3object(nullptr),object_reader(nullptr)
  {}

  void parquet_query_setting(s3select *s3_query)
  {
    if(s3_query)
    {
      m_s3_select = s3_query;
    }

    m_sa = m_s3_select->get_scratch_area();
    m_s3_select->get_scratch_area()->set_parquet_type();
    load_meta_data_into_scratch_area();
    for(auto x : m_s3_select->get_projections_list())
    {
        x->extract_columns(m_projections_columns,object_reader->get_num_of_columns());
    }

    if(m_s3_select->get_filter())
        m_s3_select->get_filter()->extract_columns(m_where_clause_columns,object_reader->get_num_of_columns());

    m_is_to_aggregate = true; //TODO when to set to true
    not_to_increase_first_time = true;
  }

  ~nb_parquet_object()
  {
    if(object_reader != nullptr)
    {
      delete object_reader;
    }

  }

  std::string get_error_description()
  {
    return m_error_description;
  }

  bool is_set()
  {
    return m_s3_select != nullptr; 
  }

  void set_parquet_object(std::string parquet_file_name, s3select *s3_query,s3selectEngine::rgw_s3select_api* rgw) //TODO duplicate code
  {
    try{
    
      object_reader = new parquet_file_parser(parquet_file_name,rgw); //TODO uniq ptr
    } catch(std::exception &e)
    { 
      throw base_s3select_exception(std::string("failure while processing parquet meta-data ") + std::string(e.what()) ,base_s3select_exception::s3select_exp_en_t::FATAL);
    }

    parquet_query_setting(s3_query);
  }
  

  int run_s3select_on_object(std::string &result,
        std::function<int(std::string&)> fp_s3select_result_format,
        std::function<int(std::string&)> fp_s3select_header_format)
  {
    int status = 0;

    do
    {
      try
      {
        status = getMatchRow(result);
      }
      catch (base_s3select_exception &e)
      {
        m_error_description = e.what();
        m_error_count++;
        if (e.severity() == base_s3select_exception::s3select_exp_en_t::FATAL || m_error_count > 100) //abort query execution
        {
          return -1;
        }
      }
      catch (std::exception &e)
      {
        m_error_description = e.what();
        m_error_count++;
        if (m_error_count > 100) //abort query execution
        {
          return -1;
        }
      }

#define S3SELECT_RESPONSE_SIZE_LIMIT (4 * 1024 * 1024)
      if (result.size() > S3SELECT_RESPONSE_SIZE_LIMIT)
      {//AWS-cli limits response size the following callbacks send response upon some threshold
        fp_s3select_result_format(result);

        if (!is_end_of_stream())
        {
          fp_s3select_header_format(result);
        }
      }
      else
      {
        if (is_end_of_stream())
        {
          fp_s3select_result_format(result);
        }
      }

      if (status < 0 || is_end_of_stream())
      {
        break;
      }

    } while (1);

    return status;
  }

  void load_meta_data_into_scratch_area()
  {
    int i=0;
    for(auto x : object_reader->get_schema())
    {
      m_s3_select->get_scratch_area()->set_column_pos(x.first.c_str(),i++); 
    }
  }

  virtual bool is_end_of_stream()
  {
    return object_reader->end_of_stream();
  }

  virtual void columnar_fetch_where_clause_columns()
  {
    if(!not_to_increase_first_time)//for rownum=0 
      object_reader->increase_rownum();
    else
      not_to_increase_first_time = false;

    auto status = object_reader->get_column_values_by_positions(m_where_clause_columns, m_predicate_values);
    if(status<0)//TODO exception?
      return;
    m_sa->update(m_predicate_values, m_where_clause_columns);
  }

  virtual void columnar_fetch_projection()
  {
    auto status = object_reader->get_column_values_by_positions(m_projections_columns, m_projections_values);
    if(status<0)//TODO exception?
      return;
    m_sa->update(m_projections_values, m_projections_columns);
  }

};

}//namespace s3selectEngine