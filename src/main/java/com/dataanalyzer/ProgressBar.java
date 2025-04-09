package com.dataanalyzer;


public class ProgressBar {
    private final int width;
    private final char barChar;
    private final char emptyChar;
    private final String format;
    private String currentStatus = "";

    
    public ProgressBar(int width, char barChar, char emptyChar, boolean showPercent) {
        this.width = width;
        this.barChar = barChar;
        this.emptyChar = emptyChar;
        this.format = showPercent ? "\r[%s] %3d%% %s" : "\r[%s] %s";
    }

  
    public ProgressBar() {
        this(50, '█', '░', true);
    }

    
    public void update(double progress, String status) {
        if (progress < 0) progress = 0;
        if (progress > 1) progress = 1;

        int filledWidth = (int) (width * progress);
        StringBuilder bar = new StringBuilder();
        
        for (int i = 0; i < width; i++) {
            bar.append(i < filledWidth ? barChar : emptyChar);
        }

        currentStatus = status != null ? status : "";
        
        if (format.contains("%3d")) {
            System.out.printf(format, bar.toString(), (int) (progress * 100), currentStatus);
        } else {
            System.out.printf(format, bar.toString(), currentStatus);
        }
    }

  
    public void update(double progress) {
        update(progress, currentStatus);
    }

    
    public void complete() {
        System.out.println();
    }

    public void complete(String completionMessage) {
        System.out.println();
        System.out.println(completionMessage);
    }
}
