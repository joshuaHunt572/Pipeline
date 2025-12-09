/pipeline
    /1_whisper
        Whisper_Inbox/
        Whisper_Output/
    /2_extractor
        Extractor_Inbox/
        Extractor_Output/
    /3_categorizer
        Categorizer_Inbox/
        Categorizer_Output/
    /4_preprocess
        Preprocess_Inbox/
        Preprocessing_Output/
    /5_prime
        Prime_Inbox/
        Prime_Output/
        Final_Output/prime/
    /6_output_engine
        Output_Inbox/
        Final_Output/structured_*/
    /7_synthesis
        Synthesis_Inbox/   (Prime_Output)
        Synthesized_Output/
        Final_Output/deliverables/
    /8_cloud_dispatch
        Dispatch_Inbox/   (Final_Output/deliverables)
        Cloud_Results/