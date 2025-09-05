#!/usr/bin/env python3
"""
Simple horizontal presentation diagram for GCP data pipeline
Designed to fit on a presentation slide
"""

from diagrams import Diagram, Edge
from diagrams.gcp.database import SQL
from diagrams.gcp.analytics import Dataflow, BigQuery
from diagrams.gcp.compute import Functions
from diagrams.gcp.devtools import Scheduler
from diagrams.gcp.storage import Storage

def create_presentation_diagram():
    """Create simple horizontal flow diagram for presentation"""
    
    with Diagram("", 
                 filename="presentation_pipeline", 
                 show=False,
                 direction="LR",
                 graph_attr={"rankdir": "LR", "splines": "ortho", "nodesep": "3.5", "ranksep": "2.5"},
                 node_attr={"shape": "plaintext", "fontcolor": "black"}):
        
        # Create components with icon above and text below
        source = SQL("SOURCE\n\nCloud SQL", 
                    fillcolor="#e3f2fd", color="#1976d2", 
                    shape="box", style="rounded,filled", penwidth="2")
        
        scheduler = Scheduler("ORCHESTRATION\n\nCloud\nScheduler", 
                             fillcolor="#e8f5e8", color="#388e3c",
                             shape="box", style="rounded,filled", penwidth="2")
        
        dataflow = Dataflow("PROCESSING\n\nDataflow\nETL", 
                           fillcolor="#fff3e0", color="#f57c00",
                           shape="box", style="rounded,filled", penwidth="2")
        
        bigquery = BigQuery("WAREHOUSE\n\nBigQuery\n+ Dataform", 
                           fillcolor="#f3e5f5", color="#7b1fa2",
                           shape="box", style="rounded,filled", penwidth="2")
        
        archive = Storage("ARCHIVE\n\nCloud Storage", 
                         fillcolor="#f3e5f5", color="#7b1fa2",
                         shape="box", style="rounded,filled", penwidth="2")
        
        reports = Functions("OUTPUT\n\nAutomated\nReports", 
                           fillcolor="#ffebee", color="#d32f2f",
                           shape="box", style="rounded,filled", penwidth="2")
        
        # Main flow
        source >> Edge(color="#6c757d", penwidth="3") >> dataflow
        dataflow >> Edge(color="#6c757d", penwidth="3") >> bigquery  
        bigquery >> Edge(color="#6c757d", penwidth="3") >> reports
        
        # Secondary flows
        scheduler >> Edge(color="#6c757d", penwidth="2") >> dataflow
        dataflow >> Edge(color="#6c757d", penwidth="1", style="dashed") >> archive

if __name__ == "__main__":
    create_presentation_diagram()
    print("✅ Presentation diagram created: presentation_pipeline.png")