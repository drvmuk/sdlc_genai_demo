from src.config import get_config
from src.utils import get_spark_session
from src.workflow import run_workflow


def main():
    """Main entry point for the workflow."""
    # Get Spark session
    spark = get_spark_session()
    
    # Get workflow configuration
    config = get_config()
    
    # Run the workflow
    run_workflow(spark, config)
    
    # Stop Spark session
    spark.stop()


if __name__ == "__main__":
    main()